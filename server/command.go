package server

import (
        "github.com/goraft/raft"
        "stripe-ctf.com/sqlcluster/sql"
        "fmt"
        "errors"
        "stripe-ctf.com/sqlcluster/util"
)

// This command writes a value to a key.
type WriteCommand struct {
        Query string `json:"query"`
}

// Creates a new write command.
func NewWriteCommand(query string) *WriteCommand {
        return &WriteCommand{
                Query:   query,
        }
}

// The name of the command in the log.
func (c *WriteCommand) CommandName() string {
        return "write"
}

// Execute an SQL statement
func (c *WriteCommand) Apply(server raft.Server) (interface{}, error) {
        sql := server.Context().(*sql.SQL)
        output, err := sql.Execute("primary",c.Query)

        if err != nil {
                var msg string
                if output != nil && len(output.Stderr) > 0 {
                        template := `Error executing %#v (%s)

SQLite error: %s`
                        msg = fmt.Sprintf(template, c.Query, err.Error(), util.FmtOutput(output.Stderr))
                } else {
                        msg = err.Error()
                }

                return nil, errors.New(msg)
        }

        formatted := fmt.Sprintf("SequenceNumber: %d\n%s",
                output.SequenceNumber, output.Stdout)

        return []byte(formatted), nil
}
