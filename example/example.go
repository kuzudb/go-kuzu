package main

import (
	"fmt"
	"os"

	"github.com/kuzudb/go-kuzu"
)

func main() {
	dbPath := "example_db"
	os.RemoveAll(dbPath)
	systemConfig := kuzu.DefaultSystemConfig()
	systemConfig.BufferPoolSize = 1024 * 1024 * 1024
	db := kuzu.OpenDatabase(dbPath, systemConfig)
	conn := kuzu.OpenConnection(db)
	queries := []string{
		"CREATE NODE TABLE User(name STRING, age INT64, PRIMARY KEY (name))",
		"CREATE NODE TABLE City(name STRING, population INT64, PRIMARY KEY (name))",
		"CREATE REL TABLE Follows(FROM User TO User, since INT64)",
		"CREATE REL TABLE LivesIn(FROM User TO City)",
		"COPY User FROM \"user.csv\"",
		"COPY City FROM \"city.csv\"",
		"COPY Follows FROM \"follows.csv\"",
		"COPY LivesIn FROM \"lives-in.csv\"",
		"MATCH (a:User)-[e:Follows]->(b:User) RETURN a.name, e.since, b.name",
	}
	for _, query := range queries {
		queryResult := conn.Query(query)
		queryResultStr := queryResult.ToString()
		fmt.Println(queryResultStr)
	}
}
