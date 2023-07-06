package main

import (
	"fmt"
	"simple-Key-Value-DB/database"
)

func main() {
	fmt.Println("Welcome to KV Database!")
	fmt.Println("------------------------------")

	database.RunKVDataBase()
}
