package batching

import (
	"encoding/binary"
	"github.com/google/uuid"
	"github.com/valyala/fasthttp"
	"github.com/vmihailenco/msgpack/v4"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

const IntByteLength = 4

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
	name       string
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
		log.Printf("Socket file (%v) already exists\n", filePath)
		if err := os.Remove(filePath); err != nil {
			log.Printf("Remove socket file error: %v\n", err)
		}
	}

	socket, err := net.Listen("unix", filePath)
	if err != nil {
		log.Fatal("Listen error: ", err)
	}

	return &Batching{
		name:       name,
		socket:     socket,
		maxLatency: maxLatency,
		batchSize:  batchSize,
		capacity:   capacity,
		timeout:    timeout,
		jobs:       make(map[string]*Job),
	}
}

func (b *Batching) HandleHTTP(ctx *fasthttp.RequestCtx) {
	data := ctx.PostBody()
	if len(data) == 0 {
		ctx.SetStatusCode(400)
		return
	}

	job := newJob(data, b.timeout)
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

	select {
	case <-job.done:
		if job.errorCode != 0 {
			ctx.SetStatusCode(job.errorCode)
		}
		ctx.SetBody(job.result)
	case <-time.After(b.timeout):
		ctx.TimeoutError("Timeout!")
	}
}

func (b *Batching) Stop() error {
	return b.socket.Close()
}

func (b *Batching) batchQuery(conn net.Conn) {
	batch := make(String2Bytes)
	job := <-b.queue
	batch[job.id] = job.data
	// timing from getting the first query data
	waitUntil := time.Now().Add(b.maxLatency)
	for time.Now().Before(waitUntil) && len(batch) < b.batchSize {
		select {
		case job := <-b.queue:
			if time.Now().After(job.expire) {
				job.errorCode = 408
				job.done <- true
				continue
			}
			batch[job.id] = job.data
		case <-time.After(time.Millisecond):
			continue
		}
	}

	data, err := msgpack.Marshal(batch)
	if err != nil {
		log.Fatal("Msgpack encode error:", err)
	}
	length := make([]byte, IntByteLength)
	binary.BigEndian.PutUint32(length, uint32(len(data)))
	_, errLen := conn.Write(length)
	_, errData := conn.Write(data)
	if errLen != nil || errData != nil{
		log.Fatal("Socket write error:", errLen, errData)
	}
}

func (b *Batching) collectResult(conn net.Conn, length uint32) {
	data := make([]byte, length)
	_, err := conn.Read(data)
	if err != nil {
		log.Fatal("Socket read error:", err)
	}

	batch := make(String2Bytes)
	err = msgpack.Unmarshal(data, batch)
	if err != nil {
		log.Fatal("Msgpack decode error:", err)
	}

	for id, result := range batch {
		b.jobsLock.Lock()
		job, ok := b.jobs[id]
		if ok {
			job.result = result
			job.done <- true
			delete(b.jobs, id)
		}
		b.jobsLock.Unlock()
	}

	// next batch
	go b.batchQuery(conn)
}

func (b *Batching) Run() {
	for {
		conn, err := b.socket.Accept()
		if err != nil {
			log.Fatal("Accept error: ", err)
		}
		log.Printf("%v accepts connection from %v\n", conn.LocalAddr(), conn.RemoteAddr())

		lengthByte := make([]byte, IntByteLength)
		if _, err := conn.Read(lengthByte); err != nil {
			log.Println("Read buffer error:", err)
			continue
		}
		length := binary.BigEndian.Uint32(lengthByte)

		if length == 0 {
			// init query
			go b.batchQuery(conn)
		} else {
			go b.collectResult(conn, length)
		}
	}
}
