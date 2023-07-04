
# LSM Tree Simple Key-Value database

Simple Key-Value database use the LSM Tree

## How to use it 
1. Simple run `go run cmd/main.go`
2. use the insert command like `Insert {key} {value}`  insert the key value pair to the database
3. use the get command like `Get {key}` get the particular key from the database


## TODO
- [ ] Change store data object type from JSON to binary
- [ ] Persistency store from the sst file which when the program restart can build the sst from the old data
- [ ] bloom filter prevent the Read the not exist data
- [ ] integration test for the database
- [ ] benchmark test for the database
- [ ] TCP connect option instead of CLI
- [ ] Key Value Transaction
- [ ] Cluster 
