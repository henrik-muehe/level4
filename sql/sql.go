package sql

import (
//	"bytes"
	"bufio"
	"os/exec"
//	"strings"
	"stripe-ctf.com/sqlcluster/log"
	"sync"
	"syscall"
)

type ClientRecord struct {
	id		string
	c 		chan string
}

type SQL struct {
	path           	string
	sequenceNumber 	int
	clients 		map[string]ClientRecord
	mutex          	sync.Mutex
	commandChan     chan string
	outputChan      chan string
}

type Output struct {
	Stdout         []byte
	Stderr         []byte
	SequenceNumber int
}

func NewSQL(path string) *SQL {
	sql := &SQL{
		path: path,
		clients: make(map[string]ClientRecord),
		commandChan: make(chan string),
		outputChan: make(chan string),
	}


	go func() {
		// Sqlite memory thread
		c := exec.Command("sqlite3","-batch")

		stdin, _ := c.StdinPipe()
		stdout, _ := c.StdoutPipe()
		//stderr, _ := c.StderrPipe()

		c.Start();

		outbr := bufio.NewReader(stdout)
		//errbr := bufio.NewReader(stderr)

		readOutput := func(br *bufio.Reader) string {
			output := ""
			for {
				line, _, err := br.ReadLine()
				if err != nil {
					panic("Unable to read from reader in SQL")
				}
				sline := string(line)
				if sline == "done" { break }
				output += sline + "\n"
			}
			return output
		}

		for {
			command := <- sql.commandChan
			//log.Printf("Received command: %s",command)
			stdin.Write([]byte(command + "\nselect 'done';\n"))
			output := readOutput(outbr)
			//log.Printf("SQLITE RETURNED %s", output)
			sql.outputChan <- output
		}
	}()


	return sql
}

func getExitstatus(err error) int {
	exiterr, ok := err.(*exec.ExitError)
	if !ok {
		return -1
	}

	status, ok := exiterr.Sys().(syscall.WaitStatus)
	if !ok {
		return -1
	}

	return status.ExitStatus()
}

func (sql *SQL) AddClientRecord(id string, c chan string) {
	sql.mutex.Lock()
	defer sql.mutex.Unlock()
	sql.clients[id]=ClientRecord{ id, c }
}

func (sql *SQL) Respond(id string, output []byte) {
	sql.mutex.Lock()
	defer sql.mutex.Unlock()

	if record, ok := sql.clients[id]; ok {
		log.Println("Respond called for %s", id);
		record.c <- string(output);
	}
}

func (sql *SQL) Execute(tag string, command string) (*Output, error) {
	sql.mutex.Lock()
	defer sql.mutex.Unlock()
	defer func() { sql.sequenceNumber += 1 }()

	sql.commandChan <- command

	output := &Output{
		Stdout:         []byte(<- sql.outputChan),
		Stderr:         nil,
		SequenceNumber: sql.sequenceNumber,
	}

	return output, nil
}
