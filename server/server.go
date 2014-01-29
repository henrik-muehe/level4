package server

import (
	"bytes"
	"fmt"
	"github.com/gorilla/mux"
	"io/ioutil"
    "math/rand"
	"net/http"
	"path/filepath"
	"github.com/goraft/raft"
	"stripe-ctf.com/sqlcluster/log"
	"stripe-ctf.com/sqlcluster/sql"
	"stripe-ctf.com/sqlcluster/transport"
	"stripe-ctf.com/sqlcluster/util"
	"encoding/json"
	"time"
)

type Server struct {
	name       string
	path       string
	listen     string
	counter    int
	received   map[string]bool
    raftServer raft.Server
	router     *mux.Router
	httpServer *http.Server
	sql        *sql.SQL
	client     *transport.Client
}

type Join struct {
	Self ServerAddress `json:"self"`
}

type JoinResponse struct {
	Self    ServerAddress   `json:"self"`
	Members []ServerAddress `json:"members"`
}

type Replicate struct {
	Self  ServerAddress `json:"self"`
	Query []byte        `json:"query"`
}

type ReplicateResponse struct {
	Self ServerAddress `json:"self"`
}

type ServerAddress struct {
	Name             string `json:"name"`
	ConnectionString string `json:"address"`
}

// Creates a new server.
func New(path, listen string) (*Server, error) {
	sqlPath := filepath.Join(path, "storage.sql")
	util.EnsureAbsent(sqlPath)

    s := &Server{
    	received: make(map[string]bool),
		path:    path,
		listen:  listen,
		sql:     sql.NewSQL(sqlPath),
		router:  mux.NewRouter(),
		client:  transport.NewClient(),
	}

	// Read existing name or generate a new one.
	if b, err := ioutil.ReadFile(filepath.Join(path, "name")); err == nil {
		s.name = string(b)
	} else {
		s.name = fmt.Sprintf("%07x", rand.Int())[0:7]
		if err = ioutil.WriteFile(filepath.Join(path, "name"), []byte(s.name), 0644); err != nil {
			panic(err)
		}
	}

	return s, nil
}

// This is a hack around Gorilla mux not providing the correct net/http
// HandleFunc() interface.
func (s *Server) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
        s.router.HandleFunc(pattern, handler)
}

// Leader returns the current leader.
func (s *Server) Leader() string {
        l := s.raftServer.Leader()

        if l == "" {
                // We are a single node cluster, we are the leader
                return s.raftServer.Name()
        }

        return l
}

// Leader returns the current leader.
func (s *Server) LeaderConnectionString() string {
	name := s.Leader()
	for n, peer := range s.raftServer.Peers() {
		if n == name {
			return peer.ConnectionString
		}
	}
    return ""
}

// IsLeader returns true if this instance the current leader.
func (s *Server) IsLeader() bool {
        return s.raftServer.State() == raft.Leader
}

// Starts the server.
func (s *Server) ListenAndServe(leader string) error {
	var err error

    log.Printf("Initializing Raft Server: %s", s.path)
             
	//time.Sleep(500 * time.Millisecond)


    // Initialize and start Raft server.
    transporter := raft.NewHTTPTransporter("/raft")
    transporter.Transport.Dial = transport.UnixDialer
    s.raftServer, err = raft.NewServer(s.name, s.path, transporter, nil, s.sql, "")
    if err != nil {
            log.Fatal(err)
    }
    transporter.Install(s.raftServer, s)


    s.raftServer.Start()
	s.raftServer.SetHeartbeatTimeout(25 * time.Millisecond)
	s.raftServer.SetElectionTimeout(150 * time.Millisecond)
	fn := func(e raft.Event) {
		log.Printf("%s %v -> %v\n", e.Type(), e.PrevValue(), e.Value())
	}
    s.raftServer.AddEventListener(raft.StateChangeEventType, fn)
    s.raftServer.AddEventListener(raft.LeaderChangeEventType, fn)
    s.raftServer.AddEventListener(raft.TermChangeEventType, fn)


    if leader != "" {
            // Join to leader if specified.

            log.Println("Attempting to join leader:", leader)

            if !s.raftServer.IsLogEmpty() {
                    log.Fatal("Cannot join with an existing log")
            }
             //time.Sleep(1 * time.Second)

            if err := s.Join(leader); err != nil {
            	log.Println("Join failed")
                log.Fatal(err)
            }
            log.Printf("Node %s joined leader %s" , s.connectionString(), leader)

    } else if s.raftServer.IsLogEmpty() {
            // Initialize the server by joining itself.

            log.Println("Initializing new cluster")

			cs, err := transport.Encode(s.listen)
			if err != nil {
				return err
			}

            _, err = s.raftServer.Do(&raft.DefaultJoinCommand{
                    Name:             s.raftServer.Name(),
                    ConnectionString: cs,
            })
            if err != nil {
                    log.Fatal(err)
            }

    } else {
            log.Println("Recovered from log")
    }

    log.Println("Initializing HTTP server")

	// Initialize and start HTTP server.
	s.httpServer = &http.Server{
		Handler: s.router,
	}

	s.router.HandleFunc("/sql", s.sqlHandler).Methods("POST")
	s.router.HandleFunc("/join", s.joinHandler).Methods("POST")

	// Start Unix transport
	l, err := transport.Listen(s.listen)
	if err != nil {
		log.Fatal(err)
	}
	return s.httpServer.Serve(l)
}

func (s *Server) connectionString() string {
	cs,err := transport.Encode(s.listen)
	if err != nil {
		log.Fatal(err)
	}
	return cs
}

// Join an existing cluster
func (s *Server) Join(primary string) error {
	cs, err := transport.Encode(primary)
	if err != nil {
		return err
	}

    command := &raft.DefaultJoinCommand{
            Name:             s.raftServer.Name(),
            ConnectionString: s.connectionString(),
    }

    for {
		b := util.JSONEncode(command)
	    _, err = s.client.SafePost(cs, "/join", b)
	    if err != nil {
			log.Printf("Unable to join cluster: %s", err)
			time.Sleep(1 * time.Second)
			continue
	    }
	    return nil
	}

    return nil
}

// Server handlers
func (s *Server) joinHandler(w http.ResponseWriter, req *http.Request) {
	command := &raft.DefaultJoinCommand{}

	if err := json.NewDecoder(req.Body).Decode(&command); err != nil {
		log.Printf("Invalid join request: %s", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	result, err := s.raftServer.Do(command)
	if err != nil {
		log.Printf("Unable to handle join: %s", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if b, ok := result.([]byte); ok {
		log.Printf(string(b))
        w.WriteHeader(http.StatusOK)
        w.Write(b)
	}
}

// This is the only user-facing function, and accordingly the body is
// a raw string rather than JSON.
func (s *Server) sqlHandler(w http.ResponseWriter, req *http.Request) {
	// Assign id
	var id string;
	var forwarded bool;
	id = req.URL.Query().Get("id")
	resChannel := make(chan string)
	if id == "" {
		// This is a client talking to us, assign a unique id
		id = fmt.Sprintf("%s-%d", s.connectionString(), s.counter)
		s.counter += 1


		forwarded = false
		s.sql.AddClientRecord(id,resChannel);

		log.Printf("Received request %s",id)
	} else {
		// This is a forwarded request
		forwarded = true

		if val, ok := s.received[id]; ok && val {
			return
		}

		log.Printf("Received forwarded request %s",id)
		s.received[id]=true
        w.WriteHeader(http.StatusOK)
	}

	// Read body
	query, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Printf("Couldn't read body: %s", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	// Forward if we are not the leader
	go func() {
		var count int = 0
		delay  := 50 * time.Millisecond
	    for {
	    	count += 1
	    	if count > 1 {
	    		log.Printf("Retry count for %s is %d",id,count)
	     	   	time.Sleep(delay)     	 
	     	   	if (delay < 1*time.Second) { delay *= 2 } 
		    }

			// Redirect if we are not the leader
			if !s.IsLeader() {
				// Find leader and make sure we do not redirect back to us
				target := s.LeaderConnectionString()
				if (target == "") { continue }
				if (target == s.name) { continue }

				// Redirect
				for {
					res, err := s.client.RawPost(target, fmt.Sprintf("/sql?id=%s",id), bytes.NewBuffer(query))
					if err == nil && res.StatusCode == 200 { break }
		     	   	time.Sleep(50 * time.Millisecond)
				}
				s.received[id] = false
				break
			} else {
			    _, err := s.raftServer.Do(NewWriteCommand(id,string(query)))

			    // Retry on error
		    	if err != nil { continue; }
		    	break
			}
	    }
    }()

    if !forwarded {
		response := <- resChannel
		log.Printf("Responding for id %s",id)
		w.Write([]byte(response))
    }
}
