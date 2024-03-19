package main

import (
	"fmt"
	"log"
	"os"
	"simple-Key-Value-DB/database"
	"strings"

	"github.com/c-bata/go-prompt"
	"github.com/c-bata/go-prompt/completer"
)

var db *database.DB

func complete(in prompt.Document) []prompt.Suggest {
	s := []prompt.Suggest{}

	return prompt.FilterHasPrefix(s, in.GetWordBeforeCursor(), true)
}

func kvDBExecutor(in string) {
	commands := strings.Split(in, " ")

	command := strings.ToUpper(commands[0])

	switch command {
	case "GET":
		if len(commands) != 2 {
			fmt.Println("Invalid command")
			return
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
			return
		}

		err := db.Insert(commands[1], commands[2])

		if err != nil {
			fmt.Println("insert error", err)
		}

	case "EXIT":
		fmt.Println("Bye!")
		os.Exit(0)
	default:
		fmt.Println("Invalid command")
	}
}

func RunKVDataBase() {
	var err error

	db, _ = database.Open()

	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	p := prompt.New(
		kvDBExecutor,
		complete,
		prompt.OptionPrefix(">> "),
		prompt.OptionTitle("simple key-value DB"),
		prompt.OptionInputTextColor(prompt.Yellow),
		prompt.OptionCompletionWordSeparator(completer.FilePathCompletionSeparator),
	)

	p.Run()
}
