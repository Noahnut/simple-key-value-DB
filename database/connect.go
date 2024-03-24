package database

import (
	"bufio"
	"fmt"
	"log"
	"net"
)

const SimpleKeyValueDefaultPort = 12345

type Connect struct {
	db       *DB
	executor *Executor
	listen   net.Listener
	port     int
}

func StartConnect(customPort int, db *DB, executor *Executor) {
	if customPort == 0 {
		customPort = SimpleKeyValueDefaultPort
	}

	li, err := net.Listen("tcp", fmt.Sprintf(":%d", customPort))

	if err != nil {
		log.Fatal(err)
	}

	c := &Connect{
		db:     db,
		port:   customPort,
		listen: li,
	}

	defer c.listen.Close()
	println("Simple Key Value DB is Listening on port:", c.port)
	for {
		conn, err := c.listen.Accept()

		if err != nil {
			log.Println(err)
			continue
		}

		go c.handle(conn)
	}
}

func (c *Connect) handle(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		line := scanner.Text()

		c.executor.Execute(line)
	}
}

func (c *Connect) Close() {
	c.listen.Close()
}
