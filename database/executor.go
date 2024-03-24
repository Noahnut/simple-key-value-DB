package database

import (
	"fmt"
	"os"
	"strings"
)

type Executor struct {
	db *DB
}

func NewExecutor(db *DB) *Executor {
	return &Executor{db: db}
}

func (e *Executor) Execute(in string) {
	commands := strings.Split(in, " ")

	command := strings.ToUpper(commands[0])

	switch command {
	case "GET":
		if len(commands) != 2 {
			fmt.Println("Invalid command")
			return
		}

		value, exist := e.db.Get(commands[1])

		if exist {
			fmt.Println(string(value))
		} else {
			fmt.Println("Key not found")
		}
	case "PUT":
	case "DELETE":
	case "INSERT":
		if len(commands) != 3 {
			fmt.Println("Invalid command")
			return
		}

		err := e.db.Insert(commands[1], commands[2])

		if err != nil {
			fmt.Println("insert error", err)
		}

	case "EXIT":
		//TODO: should do the graceful shutdown prevent any data loss
		fmt.Println("Bye!")
		os.Exit(0)
	default:
		fmt.Println("Invalid command")
	}
}
