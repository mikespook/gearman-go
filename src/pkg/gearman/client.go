package gearman

import (
    "os"
    "net"
    "log"
)

type Client struct {
    conn net.Conn
    running bool
    JobQueue chan *ClientJob
    ErrQueue chan os.Error
}

func NewClient() (client * Client){
    client = &Client{running:false,
        JobQueue:make(chan *ClientJob, QUEUE_CAP),
        ErrQueue:make(chan os.Error, QUEUE_CAP),}
    return
}

func (client *Client) AddServer(addr string) (err os.Error) {
    conn, err := net.Dial(TCP, addr)
    if err != nil {
        return
    }
    client.conn = conn
    go client.work()
    return
}

func (client *Client) work() {
    OUT: for client.running {
        var rel []byte
        for {
            buf := make([]byte, 2048)
            n, err := client.conn.Read(buf)
            if err != nil {
                if err == os.EOF && n == 0 {
                    break
                }
                client.ErrQueue <- err
                continue OUT
            }
            rel = append(rel, buf[0: n] ...)
        }
        job, err := DecodeClientJob(rel)
        if err != nil {
            client.ErrQueue <- err
        } else {
            switch(job.dataType) {
                case ERROR:
                    _, err := getError(job.Data)
                    client.ErrQueue <- err
                case ECHO_RES:
                    client.JobQueue <- job
            }
        }
    }
}

func (client *Client) Do(funcname string, data []byte, flag byte) (err os.Error) {
    return
}

func (client *Client) Echo(data []byte) (err os.Error) {
    job := NewClientJob(REQ, ECHO_REQ, data)
    return client.WriteJob(job)
}

func (client *Client) LastResult() (job *ClientJob) {
    if l := len(client.JobQueue); l != 1 {
        if l == 0 {
            return
        }
        for i := 0; i < l - 1; i ++ {
            <-client.JobQueue
        }
    }
    return <-client.JobQueue   
}

func (client *Client) LastError() (err os.Error) {
    if l := len(client.ErrQueue); l != 1 {
        if l == 0 {
            return
        }
        for i := 0; i < l - 1; i ++ {
            <-client.ErrQueue
        }
    }
    return <-client.ErrQueue
}

func (client *Client) WriteJob(job *ClientJob) (err os.Error) {
    return client.Write(job.Encode())
}

func (client *Client) Write(buf []byte) (err os.Error) {
    log.Println(buf)
    var n int
    for i := 0; i < len(buf); i += n {
        n, err = client.conn.Write(buf[i:])
        if err != nil {
            return
        }
    }
    return
}

func (client *Client) Close() (err os.Error) {
    client.running = false
    err = client.conn.Close()
    close(client.JobQueue)
    close(client.ErrQueue)
    return
}
