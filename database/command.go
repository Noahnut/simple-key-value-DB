package database

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

func RunKVDataBase() {

	db, err := Open()

	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("> ")

		input, _ := reader.ReadString('\n')

		input = strings.ReplaceAll(input, "\n", "")

		if len(input) == 0 {
			continue
		}

		commands := strings.Split(input, " ")

		command := strings.ToUpper(commands[0])

		switch command {
		case "GET":
			if len(commands) != 2 {
				fmt.Println("Invalid command")
				continue
			}

			value, exist := db.Get(commands[1])

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
				continue
			}

			err := db.Insert(commands[1], commands[2])

			if err != nil {
				fmt.Println("insert error", err)
			}

		case "EXIT":
			fmt.Println("Bye!")
			return
		default:
			fmt.Println("Invalid command")
		}
	}
}
