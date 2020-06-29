package batching

import (
	"encoding/binary"
	"fmt"
	"github.com/google/uuid"
	"github.com/valyala/fasthttp"
	"github.com/vmihailenco/msgpack/v4"
	"go.uber.org/zap"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

const (
	// IntByteLength defines the byte length of `length` for data
	IntByteLength = 4
	// UUIDLength defines the string bits length
	UUIDLength = 36
	// ErrorIDsKey defines the key for error IDs
	ErrorIDsKey = "error_ids"
)

// String2Bytes structure used in socket communication protocol
type String2Bytes map[string][]byte

// Job wrap the new request as a job waiting to be done by workers
type Job struct {
	id         string
	done       chan bool
	data       []byte // request data
	statusCode int    // HTTP Error Code
	expire     time.Time
}

func newJob(data []byte, timeout time.Duration) *Job {
	return &Job{
		id:         uuid.New().String(),
		done:       make(chan bool, 1),
		data:       data,
		statusCode: 200,
		expire:     time.Now().Add(timeout),
	}
}

// Batching provides HTTP handler and socket communication.
// It generate batch jobs when workers request and send the inference results (or error)
// to the right client.
type Batching struct {
	Address    string // socket file or "{host}:{port}"
	protocol   string // "unix" (Unix domain socket) or "tcp"
	socket     net.Listener
	maxLatency time.Duration // max latency for a batch inference to wait
	batchSize  int           // max batch size for a batch inference
	capacity   int           // the capacity of the batching queue
	timeout    time.Duration // timeout for jobs in the queue
	logger     *zap.Logger
	queue      chan *Job       // job queue
	jobs       map[string]*Job // use job id as the key to find the job
	jobsLock   sync.Mutex      // lock for jobs
}

// NewBatching creates a Batching instance
func NewBatching(address, protocol string, batchSize, capacity int, maxLatency, timeout time.Duration) *Batching {
	logger, err := zap.NewProduction()
	if err != nil {
		panic("Cannot create a zap logger")
	}
	if protocol == "unix" {
		// check the socket file (remove if already exists)
		if _, err := os.Stat(address); err == nil {
			logger.Info("Socket file already exists. Try to remove it", zap.String("address", address))
			if err := os.Remove(address); err != nil {
				logger.Fatal("Remove socket file error", zap.Error(err))
			}
		}

	}

	socket, err := net.Listen(protocol, address)
	if err != nil {
		logger.Error("Cannot listen to the socket", zap.Error(err))
		panic("Cannot listen to the socket")
	}

	logger.Info("Listen on socket", zap.String("address", address))
	return &Batching{
		Address:    address,
		protocol:   protocol,
		socket:     socket,
		maxLatency: maxLatency,
		batchSize:  batchSize,
		capacity:   capacity,
		timeout:    timeout,
		logger:     logger,
		queue:      make(chan *Job, capacity),
		jobs:       make(map[string]*Job),
	}
}

// HandleHTTP is the handler for fasthttp
func (b *Batching) HandleHTTP(ctx *fasthttp.RequestCtx) {
	data := ctx.PostBody()
	if len(data) == 0 {
		// as a naive health check
		ctx.SetStatusCode(200)
		b.logger.Info("Health check request")
		return
	}

	job := newJob(data, b.timeout)
	// append job to the queue
	b.logger.Info("Add job to the queue", zap.String("jobID", job.id))
	select {
	// append to the queue
	case b.queue <- job:
		b.jobsLock.Lock()
		b.jobs[job.id] = job
		b.jobsLock.Unlock()
	default:
		// queue is full
		b.logger.Warn("Queue is full. Return error code 429")
		ctx.SetStatusCode(429)
		job.done <- true
	}

	// waiting for job done by workers
	select {
	case <-job.done:
		ctx.SetStatusCode(job.statusCode)
		ctx.SetBody(job.data)
	case <-time.After(b.timeout):
		//  timeout
		b.jobsLock.Lock()
		delete(b.jobs, job.id)
		b.jobsLock.Unlock()
		b.logger.Warn("Delete timeout job", zap.String("jobID", job.id))
		ctx.TimeoutError("Timeout!")
	}
}

// Stop the batching service and socket, close the queue channel
func (b *Batching) Stop() error {
	b.logger.Info("Close socket and queue channel, flush logging")
	defer b.logger.Sync()
	close(b.queue)
	return b.socket.Close()
}

// send a batch jobs to the workers
func (b *Batching) send(conn net.Conn) error {
	batch := make(String2Bytes)
	job := <-b.queue
	batch[job.id] = job.data
	// timing from getting the first query data
	waitUntil := time.Now().Add(b.maxLatency)
	// check latency and batch size
	for time.Now().Before(waitUntil) && len(batch) < b.batchSize {
		select {
		case job := <-b.queue:
			// expired job
			if time.Now().After(job.expire) {
				b.logger.Info("Job already expired before sent to the worker", zap.String("jobID", job.id))
				job.statusCode = 408
				job.done <- true
				continue
			}
			// append job to the batch
			b.logger.Info("Job prepared to be sent", zap.String("jobID", job.id))
			batch[job.id] = job.data
		case <-time.After(time.Millisecond):
			// minimal interval: millisecond
			continue
		}
	}

	data, err := msgpack.Marshal(batch)
	if err != nil {
		b.logger.Fatal("Msgpack encode error", zap.Error(err))
	}
	length := make([]byte, IntByteLength)
	binary.BigEndian.PutUint32(length, uint32(len(data)))
	// write length and packed data to the socket
	_, errLen := conn.Write(length)
	_, errData := conn.Write(data)
	if errLen != nil || errData != nil {
		b.logger.Warn("Socket write error", zap.Error(errLen), zap.Error(errData))
		return fmt.Errorf("conn error: %v + %v", errLen, errData)
	}
	return nil
}

// receive results from workers
func (b *Batching) receive(conn net.Conn, length uint32) error {
	b.logger.Info("Received bytes length", zap.Uint32("length", length))
	data := make([]byte, length)
	_, err := conn.Read(data)
	if err != nil {
		return err
	}

	batch := make(String2Bytes)
	err = msgpack.Unmarshal(data, &batch)
	if err != nil {
		b.logger.Fatal("Msgpack decode error", zap.Error(err))
	}
	b.logger.Debug("Received data", zap.ByteString("data", data))

	b.jobsLock.Lock()
	// validation errors
	errors, ok := batch[ErrorIDsKey]
	if ok {
		for i := UUIDLength; i <= len(errors); i += UUIDLength {
			id := string(errors[i-36 : i])
			job, exist := b.jobs[id]
			if exist {
				job.statusCode = 422
				b.logger.Info("Validation error for job", zap.Int("statusCode", job.statusCode))
			}
		}
	}
	// inference result
	for id, result := range batch {
		job, ok := b.jobs[id]
		if ok {
			b.logger.Info("Job is done", zap.String("jobID", job.id))
			job.data = result
			job.done <- true
			delete(b.jobs, id)
		}
		// job may already timeout and return
	}
	b.jobsLock.Unlock()

	// next batch
	return b.send(conn)
}

// Run the socket communication
func (b *Batching) Run() {
	for {
		// accept socket connection
		conn, err := b.socket.Accept()
		if err != nil {
			b.logger.Fatal("Accept error: ", zap.Error(err))
		}
		b.logger.Info("Accept socket connection",
			zap.String("local", conn.LocalAddr().String()),
			zap.String("remote", conn.RemoteAddr().String()),
		)

		go func(conn net.Conn) {
			lengthByte := make([]byte, IntByteLength)
			for {
				if _, err := conn.Read(lengthByte); err != nil {
					if err == io.EOF {
						// usually this means the worker is dead
						b.logger.Warn("EOF")
						break
					}
					b.logger.Warn("Read buffer error", zap.Error(err))
					continue
				}
				length := binary.BigEndian.Uint32(lengthByte)

				if length == 0 {
					// init query from the worker
					err = b.send(conn)
				} else {
					// receive results from workers
					err = b.receive(conn, length)
				}

				if err != nil {
					b.logger.Warn("Socket error", zap.Error(err))
					break
				}
			}
		}(conn)
	}
}
