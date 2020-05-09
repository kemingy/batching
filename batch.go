package batching

import (
	"encoding/binary"
	"fmt"
	"github.com/google/uuid"
	"github.com/valyala/fasthttp"
	"github.com/vmihailenco/msgpack/v4"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

const (
	IntByteLength = 4
	UUIDLength    = 36
	ErrorIDsKey   = "error_ids"
)

type String2Bytes map[string][]byte

type Job struct {
	id        string
	done      chan bool
	data      []byte
	result    []byte
	errorCode int
	expire    time.Time
}

func newJob(data []byte, timeout time.Duration) *Job {
	return &Job{
		id:     uuid.New().String(),
		done:   make(chan bool, 1),
		data:   data,
		expire: time.Now().Add(timeout),
	}
}

type Batching struct {
	Name       string
	socket     net.Listener
	maxLatency time.Duration
	batchSize  int
	capacity   int
	timeout    time.Duration
	queue      chan *Job
	jobs       map[string]*Job
	jobsLock   sync.Mutex
}

func NewBatching(name string, batchSize, capacity int, maxLatency, timeout time.Duration) *Batching {
	filePath := name + ".socket"
	if _, err := os.Stat(filePath); err == nil {
		log.Printf("Socket file (%v) already exists. Try to remove it\n", filePath)
		if err := os.Remove(filePath); err != nil {
			log.Printf("Remove socket file error: %v\n", err)
		}
	}

	socket, err := net.Listen("unix", filePath)
	if err != nil {
		log.Fatal("Listen error: ", err)
	}

	log.Printf("Listen on %s.socket\n", name)
	return &Batching{
		Name:       name,
		socket:     socket,
		maxLatency: maxLatency,
		batchSize:  batchSize,
		capacity:   capacity,
		timeout:    timeout,
		queue:      make(chan *Job, capacity),
		jobs:       make(map[string]*Job),
	}
}

func (b *Batching) HandleHTTP(ctx *fasthttp.RequestCtx) {
	data := ctx.PostBody()
	if len(data) == 0 {
		// as a naive health check
		ctx.SetStatusCode(200)
		log.Println("Health check request")
		return
	}

	job := newJob(data, b.timeout)
	// append job to the queue
	log.Println("Add job to the queue: ", job.id)
	select {
	case b.queue <- job:
		b.jobsLock.Lock()
		b.jobs[job.id] = job
		b.jobsLock.Unlock()
	default:
		// queue is full
		log.Println("Queue is full. Return error code 429.")
		ctx.SetStatusCode(429)
		job.done <- true
	}

	// waiting for job done by workers
	select {
	case <-job.done:
		if job.errorCode != 0 {
			ctx.SetStatusCode(job.errorCode)
		}
		ctx.SetBody(job.result)
	case <-time.After(b.timeout):
		//  timeout
		b.jobsLock.Lock()
		delete(b.jobs, job.id)
		b.jobsLock.Unlock()
		log.Println("Delete timeout job: ", job.id)
		ctx.TimeoutError("Timeout!")
	}
}

func (b *Batching) Stop() error {
	close(b.queue)
	return b.socket.Close()
}

func (b *Batching) send(conn net.Conn) error {
	batch := make(String2Bytes)
	job := <-b.queue
	batch[job.id] = job.data
	// timing from getting the first query data
	waitUntil := time.Now().Add(b.maxLatency)
	for time.Now().Before(waitUntil) && len(batch) < b.batchSize {
		select {
		case job := <-b.queue:
			if time.Now().After(job.expire) {
				log.Println("Job already expired before sent to the worker: ", job.id)
				job.errorCode = 408
				job.done <- true
				continue
			}
			log.Println("Job prepared to be sent: ", job.id)
			batch[job.id] = job.data
		case <-time.After(time.Millisecond):
			continue
		}
	}

	data, err := msgpack.Marshal(batch)
	if err != nil {
		log.Fatal("Msgpack encode error: ", err)
	}
	length := make([]byte, IntByteLength)
	binary.BigEndian.PutUint32(length, uint32(len(data)))
	_, errLen := conn.Write(length)
	_, errData := conn.Write(data)
	if errLen != nil || errData != nil {
		log.Println("Socket write error: ", errLen, errData)
		return fmt.Errorf("conn error: %v + %v", errLen, errData)
	}
	return nil
}

func (b *Batching) receive(conn net.Conn, length uint32) error {
	log.Println("Received bytes length: ", length)
	data := make([]byte, length)
	_, err := conn.Read(data)
	if err != nil {
		return err
	}

	batch := make(String2Bytes)
	err = msgpack.Unmarshal(data, &batch)
	if err != nil {
		log.Fatal("Msgpack decode error: ", err)
	}
	log.Println("Received data: ", data)

	b.jobsLock.Lock()
	// validation errors
	errors, ok := batch[ErrorIDsKey]
	if ok {
		for i := UUIDLength; i <= len(errors); i += UUIDLength {
			id := string(errors[i-36 : i])
			job, exist := b.jobs[id]
			if exist {
				job.errorCode = 422
				log.Println("Validation error for job: ", job.id)
			}
		}
	}
	// inference result
	for id, result := range batch {
		job, ok := b.jobs[id]
		if ok {
			log.Println("Job is done: ", job.id)
			job.result = result
			job.done <- true
			delete(b.jobs, id)
		}
	}
	b.jobsLock.Unlock()

	// next batch
	return b.send(conn)
}

func (b *Batching) Run() {
	for {
		conn, err := b.socket.Accept()
		if err != nil {
			log.Fatal("Accept error: ", err)
		}
		log.Printf("%v accepts connection from %v\n", conn.LocalAddr(), conn.RemoteAddr())

		go func(conn net.Conn) {
			lengthByte := make([]byte, IntByteLength)
			for {
				if _, err := conn.Read(lengthByte); err != nil {
					if err == io.EOF {
						log.Println("EOF.")
						break
					}
					log.Println("Read buffer error:", err)
					continue
				}
				length := binary.BigEndian.Uint32(lengthByte)

				if length == 0 {
					// init query
					err = b.send(conn)
				} else {
					err = b.receive(conn, length)
				}

				if err != nil {
					log.Println(err)
					break
				}
			}
		}(conn)
	}
}
