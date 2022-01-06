/* Borrowed from: github.com/draxil/gearman_admin */
package admin

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
	"strconv"
	"regexp"
)

// Connection to a gearman server
type Connection struct {
	net.Conn
}

var colrx  *regexp.Regexp

func init(){
	colrx = regexp.MustCompile("[ \t]+")
}

// Connect to a gearman server
// gearadmin, err := gearman_admin.Connect("tcp", "gearman:4730")
func Connect(network, address string) (connection *Connection, err error) {
	connection = &Connection{}
	connection.Conn, err = net.Dial(network, address)

	if err != nil {
		err = fmt.Errorf("Error connecting to gearman server: %s", err.Error())
		return
	}

	return
}

// Query connected workers list
func (c *Connection) Workers() (workers []Worker, err error) {
	_, err = c.Write([]byte("workers\n"))
	if err != nil {
		return nil, err
	}

	var lines []string
	lines, err = read_response(c)
	if err != nil {
		return nil, err
	}

	workers, err = workers_from_lines(lines)

	return workers, err
}

// Query known tasks and their statuses
func (c *Connection) Status() (statuses []FunctionStatus, err error) {
	_, err = c.Write([]byte("status\n"))
	if err != nil {
		err = fmt.Errorf("Error requesting function status list: %s", err.Error())
		return
	}

	var lines []string
	lines, err = read_response(c)

	if err != nil {
		err = fmt.Errorf("Error getting function status list: %s", err.Error())
	}

	statuses, err = func_statuses_from_lines(lines)

	if err != nil {
		err = fmt.Errorf("Error getting function status list: %s", err.Error())
		statuses = []FunctionStatus{}
	}

	return
}

func process_line( line string ) []string{
	parts := colrx.Split(line, -1)

	return parts
}

func func_statuses_from_lines(lines []string) (funcs []FunctionStatus, err error){
	funcs = make([]FunctionStatus, 0, len(lines))

	for _, line := range lines {
		parts := process_line(line)

		if len(parts) < 3 {
			err = ProtocolError("Incomplete status entry only " + strconv.Itoa(len(parts)) + " fields found: " + line)
			return
		}

		var fs FunctionStatus

		fs.Name = parts[0]

		var unfinished, running, workers int

		unfinished, err = strconv.Atoi(parts[1])
		if err != nil {
			err = ProtocolError("bad unfinished count format: " + err.Error())
			return
		}

		running, err = strconv.Atoi(parts[2])

		if err != nil {
			err = ProtocolError("bad running count format: " + err.Error())
			return
		}

		workers, err = strconv.Atoi(parts[3])
		if err != nil {
			err = ProtocolError("bad worker count format: " + err.Error())
			return
		}

		fs.UnfinishedJobs = unfinished
		fs.RunningJobs = running
		fs.Workers = workers

		funcs = append(funcs, fs)
	}

	return

}

func workers_from_lines(lines []string) (workers []Worker, err error) {
	workers = make([]Worker, 0, len(lines))

	for _, line := range lines {
		parts := process_line(line)

		if len(parts) < 4 {
			err = ProtocolError("Incomplete worker entry")
			return
		}

		if parts[3] != `:` {
			err = ProtocolError("Malformed worker entry '" + parts[3] + "'")
			return
		}

		worker := Worker {
			Fd       : parts[0],
			Addr     : parts[1],
			ClientId : parts[2],
		}

		if len(parts) > 4 {
			worker.Functions = parts[4:]
		}

		workers = append(workers, worker)
	}

	return
}


// Decoded description of a gearman worker
type Worker struct {
	Fd          string // File descriptor
	Addr        string // IP address
	ClientId    string // Client ID
	Functions []string // List of functions
}

// Check a worker for a particular function
func (w *Worker) HasFunction(funcname string) bool {
	for _, v := range w.Functions {
		if v == funcname {
			return true
		}
	}
	return false
}

func read_response(r io.Reader) (lines []string, err error) {
	rdr := bufio.NewReader(r)
	lines = make([]string, 0, 0)
	for {
		line := ""
		if line, err = rdr.ReadString('\n'); err != nil {
			return nil, err

		} else if line == ".\n" {
			return lines, nil

		} else {
			lines = append(lines, strings.TrimRight(line, "\n"))

		}
	}

	return lines, nil
}

// Decoded description of a functions current status
type FunctionStatus struct {
	Name           string // Function name
	UnfinishedJobs int    // Number of unfinished jobs
	RunningJobs    int    // Number of running jobs
	Workers        int    // Number of workers available
}

// Protocol error
type ProtocolError string

func (p ProtocolError) Error() string {
	return "ProtoFail: " + string(p)
}
