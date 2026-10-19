// gwproxy is a TCP proxy for NATS gateway connections that can isolate a cluster.
//
// Every gateway connection of the local super-cluster goes through it (the servers
// advertise the proxy ports), so "isolating" a cluster means closing and refusing
// every gateway connection to or from that cluster: a network partition between
// clusters, while routes inside each cluster and client connections keep working.
//
//	gwproxy -control 127.0.0.1:9100 west:127.0.0.1:9111:127.0.0.1:7111 ...
//
// Control endpoint:
//
//	GET  /status                   isolated clusters and live connections
//	POST /isolate?cluster=east     drop and refuse gateway links to/from east
//	POST /heal[?cluster=east]      lift the isolation (all clusters when omitted)
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

// mapping is one proxied gateway listener: the cluster of the target server,
// the proxy's listen address and the server's real gateway address.
type mapping struct{ cluster, listen, target string }

type link struct {
	from, to string // cluster of the connecting server, cluster of the target server
	client   net.Conn
	server   net.Conn
}

var (
	mu       sync.Mutex
	isolated = map[string]bool{}
	links    = map[*link]struct{}{}
	// The connecting server identifies its cluster in its CONNECT line.
	connectRe = regexp.MustCompile(`"gateway"\s*:\s*"([^"]+)"`)
)

func main() {
	control := flag.String("control", "127.0.0.1:9100", "control HTTP endpoint")
	flag.Parse()
	if flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "usage: gwproxy [-control addr] cluster:listen:target ...")
		os.Exit(2)
	}
	for _, arg := range flag.Args() {
		parts := strings.Split(arg, ":")
		if len(parts) != 5 {
			log.Fatalf("bad mapping %q, want cluster:host:port:host:port", arg)
		}
		m := mapping{cluster: parts[0], listen: parts[1] + ":" + parts[2], target: parts[3] + ":" + parts[4]}
		ln, err := net.Listen("tcp", m.listen)
		if err != nil {
			log.Fatal(err)
		}
		go serve(ln, m)
	}

	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		var iso, ls []string
		for c := range isolated {
			iso = append(iso, c)
		}
		for l := range links {
			ls = append(ls, l.from+" -> "+l.to)
		}
		json.NewEncoder(w).Encode(map[string]any{"isolated": iso, "links": ls})
	})
	http.HandleFunc("/isolate", func(w http.ResponseWriter, r *http.Request) {
		c := r.URL.Query().Get("cluster")
		if c == "" {
			http.Error(w, "cluster required", 400)
			return
		}
		mu.Lock()
		isolated[c] = true
		n := 0
		for l := range links {
			if l.from == c || l.to == c {
				l.client.Close()
				l.server.Close()
				n++
			}
		}
		mu.Unlock()
		log.Printf("isolated %s, closed %d gateway connections", c, n)
		fmt.Fprintf(w, "isolated %s, closed %d gateway connections\n", c, n)
	})
	http.HandleFunc("/heal", func(w http.ResponseWriter, r *http.Request) {
		c := r.URL.Query().Get("cluster")
		mu.Lock()
		if c == "" {
			isolated = map[string]bool{}
		} else {
			delete(isolated, c)
		}
		mu.Unlock()
		log.Printf("healed %q", c)
		fmt.Fprintf(w, "healed\n")
	})
	log.Printf("control endpoint on http://%s", *control)
	log.Fatal(http.ListenAndServe(*control, nil))
}

func serve(ln net.Listener, m mapping) {
	for {
		c, err := ln.Accept()
		if err != nil {
			log.Printf("accept on %s: %v", m.listen, err)
			return
		}
		go handle(c, m)
	}
}

func handle(client net.Conn, m mapping) {
	mu.Lock()
	refuse := isolated[m.cluster]
	mu.Unlock()
	if refuse {
		client.Close()
		return
	}
	server, err := net.Dial("tcp", m.target)
	if err != nil {
		client.Close()
		return
	}
	// The server speaks first (INFO); forward it while we wait for the client's CONNECT.
	go func() {
		io.Copy(client, server)
		client.Close()
	}()
	br := bufio.NewReader(client)
	client.SetReadDeadline(time.Now().Add(10 * time.Second))
	line, err := br.ReadString('\n')
	client.SetReadDeadline(time.Time{})
	if err != nil {
		client.Close()
		server.Close()
		return
	}
	from := "?"
	if sm := connectRe.FindStringSubmatch(line); sm != nil {
		from = sm[1]
	}
	l := &link{from: from, to: m.cluster, client: client, server: server}
	mu.Lock()
	if isolated[from] || isolated[m.cluster] {
		mu.Unlock()
		client.Close()
		server.Close()
		return
	}
	links[l] = struct{}{}
	mu.Unlock()

	server.Write([]byte(line))
	io.Copy(server, br)
	server.Close()
	client.Close()
	mu.Lock()
	delete(links, l)
	mu.Unlock()
}
