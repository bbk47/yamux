package main

import (
	"fmt"
	"io"
	"log"
	"net"

	"github.com/hashicorp/yamux"
)

func main() {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("listen failed: %v", err)
	}
	defer listener.Close()

	port := listener.Addr().(*net.TCPAddr).Port
	fmt.Printf("READY %d\n", port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}

		go handleConn(conn)
	}
}

func handleConn(conn net.Conn) {
	defer conn.Close()

	session, err := yamux.Server(conn, nil)
	if err != nil {
		return
	}
	defer session.Close()

	for {
		stream, err := session.AcceptStream()
		if err != nil {
			return
		}

		go func(s net.Conn) {
			defer s.Close()
			_, _ = io.Copy(s, s)
		}(stream)
	}
}
