package sql

import (
	"bufio"
	"os/exec"
	"sync"
)

type ClientRecord struct {
	id		string
	c 		chan string
}


type Job struct {
	Command string
	OutChan chan string
	Seq int
}

type SQL struct {
	path           	string
	SequenceNumber 	int
	clients 		map[string]ClientRecord
	mutex          	sync.Mutex
	commandChan     chan *Job
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
		commandChan: make(chan *Job),
	}

	go func() {
		// Sqlite memory thread
		c := exec.Command("sqlite3","-batch")
		stdin, _ := c.StdinPipe()
		stdout, _ := c.StdoutPipe()

		c.Start();
		outbr := bufio.NewReader(stdout)

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
			job := <- sql.commandChan
			stdin.Write([]byte(job.Command + "\nselect 'done';\n"))
			output := readOutput(outbr)
			job.Seq =sql.SequenceNumber
			sql.SequenceNumber += 1
			job.OutChan <- output
		}
	}()

	return sql
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
		record.c <- string(output);
	}
}

func (sql *SQL) Execute(command string) *Job {
	job := &Job{command,make(chan string),-1}
	sql.commandChan <- job
	return job;
}
