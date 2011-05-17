package gearman

import (
    "net"
    "os"
    "log"
)

type JobClient struct {
    conn net.Conn
    incoming chan *Job
    running bool
}

func NewJobClient(addr string, incoming chan *Job) (jobclient *JobClient, err os.Error) {
    conn, err := net.Dial(TCP, addr)
    if err != nil {
        return nil, err
    }
    jobclient = &JobClient{conn: conn, incoming: incoming, running:true}
    return jobclient, err
}

func (server *JobClient) Work() (err os.Error) {
    for server.running {
        var rel []byte
        for {
            buf := make([]byte, 2048)
            n, err := server.conn.Read(buf)
            if err != nil {
                if err == os.EOF && n == 0 {
                    break
                }
                return err
            }
            rel = append(rel, buf[0: n] ...)
            break
        }
        job, err := DecodeJob(server, rel)
        if err != nil {
            log.Println(err)
        } else {
            server.incoming <- job
        }
    }
    return
}

func (server *JobClient) WriteJob(job * Job) (err os.Error) {
    return server.Write(job.Encode())
}

func (server *JobClient) Echo(str []byte) (err os.Error) {
    job := NewJob(server, REQ, ECHO_REQ, []byte(str))
    return server.Write(job.Encode());
}

func (server *JobClient) Write(buf []byte) (err os.Error) {
    var n int
    for i := 0; i < len(buf); i += n {
        n, err = server.conn.Write(buf[i:])
        if err != nil {
            return err
        }
    }
    return
}

func (server *JobClient) Close() (err os.Error) {
    server.running = false
    err = server.conn.Close()
    return
}
