package main

import (
    "net"
    "log"
    "fmt"
)

type Client struct {
    c * net.TCPConn
    w chan []byte
    r chan []byte
}

func NewClient() * Client {
    client := new(Client)
    wch := make(chan []byte, 2048)
    rch := make(chan []byte, 2048)
    client.w = wch
    client.r = rch
    addr, err := net.ResolveTCPAddr("127.0.0.1:4730")
    if err != nil {
        log.Panic(err.String())
    }
    c, err := net.DialTCP("tcp", nil, addr)
    client.c = c
    if err != nil {
        log.Panic(err.String())
    }
    go client.ReadLoop()
    go client.WriteLoop()
    return client
}

func (client * Client) ReadLoop() {
    for {
        buf := make([]byte, 2048)
        _, err := client.c.Read(buf)
        if err != nil {
            client.c.Close()
            break;
        }
        client.r <- buf
    }    
}

func (client * Client) WriteLoop() {
    for {
        select {
            case buf := <- client.w:
                client.c.Write(buf)            
        }
    }
}

func (client * Client) Write(data []byte) {
    client.w <- data
}

func (client * Client) Read() []byte{
    var buf []byte
    select {
        case buf = <- client.r:
    }
    return buf
}

func main() {
    client := NewClient()
    client.Write([]byte("TEST\r\n"))
    for {
        fmt.Println(client.Read())
    }
}
