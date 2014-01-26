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

// Creates a new server.
func New(path, listen string) (*Server, error) {
	sqlPath := filepath.Join(path, "storage.sql")
	util.EnsureAbsent(sqlPath)

    s := &Server{
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

    // Initialize and start Raft server.
    transporter := raft.NewHTTPTransporter("/raft")
    transporter.Transport.Dial = transport.UnixDialer
    s.raftServer, err = raft.NewServer(s.name, s.path, transporter, nil, s.sql, "")
    if err != nil {
            log.Fatal(err)
    }
    transporter.Install(s.raftServer, s)
    s.raftServer.Start()

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
	//s.router.HandleFunc("/status?id={id}", s.statusHandler).Methods("POST")

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

func (s *Server) statusHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	f,ok := s.sql.GetForward(vars["id"])
	if !ok {
		w.WriteHeader(404)
	} else {
		w.WriteHeader(f.Status)
		w.Write([]byte(f.Body))
	}
}


// This is the only user-facing function, and accordingly the body is
// a raw string rather than JSON.
func (s *Server) sqlHandler(w http.ResponseWriter, req *http.Request) {
	var id string;
	var forwarded bool;
	id = req.URL.Query().Get("id")
	if id == "" {
		// This is a client talking to us, assign a unique id
		id = fmt.Sprintf("%s-%d", s.connectionString(), s.counter)
		s.counter += 1
		log.Printf("Received request %s",id)
	} else {
		// This is a forwarded request
		forwarded = true
		log.Printf("Received forwarded request %s",id)
		s.sql.UpdateForward(id, 1, "")
	}

	query, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Printf("Couldn't read body: %s", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	var count int = 0
    for {
    	count += 1
    	if count > 1 {
     	   time.Sleep(100 * time.Millisecond)
	    }

		// Redirect if we are not the leader
		if !s.IsLeader() {
			// Find leader and make sure we do not redirect back to us
			target := s.LeaderConnectionString()
			if (target == s.name) { continue }
			if (target == "") { continue }

			//log.Printf("Node %s forwards request %s",s.connectionString(), id)

			// Redirect
			res, err := s.client.RawPost(target, fmt.Sprintf("/sql?id=%s",id), bytes.NewBuffer(query))

			// Retry on communication error
			if err != nil {
				// Communication failed, we have to check if a) the request got through
				// and b) we have to find out what the result is
				for {
					res, err = s.client.RawPost(target, fmt.Sprintf("/status?id=%s",id), nil)
					if err == nil && (res.StatusCode > 10) {
						break; 
					}
				}
			}
			defer res.Body.Close()

			if res.StatusCode != 200 {
				// Return error conditions to caller
				//http.Error(w,string(query), http.StatusBadRequest)
				//return
				continue
			}	

			// Successfully forwarded, we are done		
			body,err := ioutil.ReadAll(res.Body)
			w.Write(body)

			return
		} else {
			//log.Printf("Node %s handles request %s",s.connectionString(), id)
		    // Execute the command against the Raft server if we are the leader
		    res, err := s.raftServer.Do(NewWriteCommand(string(query)))

	    	if err != nil {
	    		//if (forwarded) {  s.sql.UpdateForward(id, 400, err.Error()) }

				//log.Printf("Node %s failed at request %s",s.connectionString(), id)
	    		// Send error if we can not currently process the request
				//log.Printf("Unable to handle execute request: %s", err)
				//http.Error(w, err.Error(), http.StatusBadRequest)
				continue
	    	}

    		if (forwarded) { s.sql.UpdateForward(id, 200, string(res.([]byte))) }
			//log.Printf("Node %s finished request %s",s.connectionString(), id)
			//log.Printf(string(res.([]byte)))

	    	w.Write(res.([]byte))
			return
		}

    }
}
