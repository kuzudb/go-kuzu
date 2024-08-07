package main

import (
	"fmt"
	"os"
	"time"

	"github.com/kuzudb/go-kuzu"
)

func main() {
	d := time.Now()
	fmt.Println(int(d.Month()), int(d.Day()), int(d.Year()))
	dbPath := "example_db"
	os.RemoveAll(dbPath)
	systemConfig := kuzu.DefaultSystemConfig()
	systemConfig.BufferPoolSize = 1024 * 1024 * 1024
	db, err := kuzu.OpenDatabase(dbPath, systemConfig)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	conn, err := kuzu.OpenConnection(db)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	fmt.Println("MaxNumThreads:", conn.GetMaxNumThreads())
	conn.SetMaxNumThreads(4)
	fmt.Println("MaxNumThreads:", conn.GetMaxNumThreads())
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
		fmt.Println("Query:", query)
		queryResult, err := conn.Query(query)
		if err != nil {
			fmt.Println(err)
			continue
		}
		defer queryResult.Close()
		queryResultStr := queryResult.ToString()
		fmt.Println(queryResultStr)
	}

	preparedStatement, err := conn.Prepare("MATCH (a:User)-[e:Follows]->(b:User) WHERE a.name = $1 RETURN a.name, e.since, b.name")
	if err != nil {
		fmt.Println("Prepare error:", err, preparedStatement)
	}
	defer preparedStatement.Close()
	args := map[string]interface{}{
		"1": "Adam",
	}
	queryResult, err := conn.Execute(preparedStatement, args)
	if err != nil {
		fmt.Println(err)
	}
	defer queryResult.Close()
	fmt.Println(queryResult.ToString())
}
