package sql

import (
	"bytes"
	"os/exec"
	"strings"
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
	}
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
	if tag == "primary" || log.Verbose() {
		//log.Printf("[%s] [%d] Executing %#v", tag, sql.sequenceNumber, command)
	}

	subprocess := exec.Command("sqlite3", sql.path)
	subprocess.Stdin = strings.NewReader(command + ";")

	var stdout, stderr bytes.Buffer
	subprocess.Stdout = &stdout
	subprocess.Stderr = &stderr

	if err := subprocess.Start(); err != nil {
		log.Panic(err)
	}

	var o, e []byte

	if err := subprocess.Wait(); err != nil {
		exitstatus := getExitstatus(err)
		switch true {
		case exitstatus < 0:
			log.Panic(err)
		case exitstatus == 1:
			fallthrough
		case exitstatus == 2:
			o = stderr.Bytes()
			e = nil
		}
	} else {
		o = stdout.Bytes()
		e = stderr.Bytes()
	}

	output := &Output{
		Stdout:         o,
		Stderr:         e,
		SequenceNumber: sql.sequenceNumber,
	}

	return output, nil
}
