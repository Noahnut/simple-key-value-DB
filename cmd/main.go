package main

import (
	"log"
	"simple-Key-Value-DB/database"

	"github.com/c-bata/go-prompt"
)

func main() {
	RunKVDataBase()
}

func complete(in prompt.Document) []prompt.Suggest {
	s := []prompt.Suggest{}

	return prompt.FilterHasPrefix(s, in.GetWordBeforeCursor(), true)
}

func RunKVDataBase() {

	db, err := database.Open()

	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	executor := database.NewExecutor(db)

	database.StartConnect(database.SimpleKeyValueDefaultPort, db, executor)
}
