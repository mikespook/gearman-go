// Copyright 2011 Xing Xing <mikespook@gmail.com>.
// All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package client

import (
)

type poolItem struct {

}

/* 
The client pool

usage:
pool := client.NewPool()
pool.Add("127.0.0.1:4730", 1)
handle := pool.Do("ToUpper", []byte("abcdef"), JOB_LOW|JOB_BG)

*/
type Pool struct {
}

// Create a new client.
// Connect to "addr" through "network"
// Eg.
//      client, err := client.New("127.0.0.1:4730")
func NewPool() (pool *Pool) {
}

func (pool *Pool) Add(addr string, rate int) {
    // init a poolItem with Client & rate
}

// Do the function.
// funcname is a string with function name.
// data is encoding to byte array.
// flag set the job type, include running level: JOB_LOW, JOB_NORMAL, JOB_HIGH,
// and if it is background job: JOB_BG.
// JOB_LOW | JOB_BG means the job is running with low level in background.
func (pool *Pool) Do(funcname string, data []byte, flag byte) (handle string, err error) {
    // Select a job server
}

// Get job status from job server.
// !!!Not fully tested.!!!
func (pool *Pool) Status(handle string) {
    //
}

// Send a something out, get the samething back.
func (pool *Pool) Echo(data []byte) {
}

// Close
func (client *Client) Close() (err error) {
}
