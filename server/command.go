package server

import (
        "github.com/goraft/raft"
        "stripe-ctf.com/sqlcluster/sql"
        "fmt"
        "time"
        "log"
)

// This command writes a value to a key.
type WriteCommand struct {
        Id      string  `json:"id"`
        Query   string  `json:"query"`
}

// Creates a new write command.
func NewWriteCommand(id string, query string) *WriteCommand {
        return &WriteCommand{
                Id:      id,
                Query:   query,
        }
}

// The name of the command in the log.
func (c *WriteCommand) CommandName() string {
        return "write"
}

func trace(s string) (string, time.Time) {
    log.Println("START:", s)
    return s, time.Now()
}

func un(s string, startTime time.Time) {
    endTime := time.Now()
    log.Println("  END:", s, "ElapsedTime in seconds:", endTime.Sub(startTime))
}

// Execute an SQL statement
func (c *WriteCommand) Apply(server raft.Server) (interface{}, error) {
        sql := server.Context().(*sql.SQL)
        job := sql.Execute(c.Query)

        go func() {
                output := []byte(<- job.OutChan)
                seq := job.Seq

                formatted := fmt.Sprintf("SequenceNumber: %d\n%s",
                        seq, output)

                sql.Respond(c.Id, []byte(formatted))
        }()

        return []byte(""), nil
}
