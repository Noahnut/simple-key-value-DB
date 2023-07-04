package main

import (
	"fmt"
	"simple-memDB/database"
)

func main() {
	fmt.Println("Welcome to KV Database!")
	fmt.Println("------------------------------")

	database.RunKVDataBase()
}
