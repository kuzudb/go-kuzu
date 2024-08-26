package main

import (
	"fmt"
	"os"
	"reflect"
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
		// "CREATE RDFGraph T;",
		// "CREATE (:T_l {val:cast(12, \"INT64\")}), (:T_l {val:cast(43, \"INT32\")}), (:T_l {val:cast(33, \"INT16\")}), (:T_l {val:cast(2, \"INT8\")}), (:T_l {val:cast(90, \"UINT64\")}), (:T_l {val:cast(77, \"UINT32\")}), (:T_l {val:cast(12, \"UINT16\")}), (:T_l {val:cast(1, \"UINT8\")}), (:T_l {val:cast(4.4, \"DOUBLE\")}), (:T_l {val:cast(1.2, \"FLOAT\")}), (:T_l {val:true}), (:T_l {val:\"hhh\"}), (:T_l {val:cast(\"2024-01-01\", \"DATE\")}), (:T_l {val:cast(\"2024-01-01 11:25:30Z+00:00\", \"TIMESTAMP\")}), (:T_l {val:cast(\"2 day\", \"INTERVAL\")}), (:T_l {val:cast(\"\\\\xB2\", \"BLOB\")});",
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
		for queryResult.HasNext() {
			tuple, err := queryResult.Next()
			if err != nil {
				fmt.Println(err)
				break
			}
			fmt.Println(tuple.GetAsString())
			defer tuple.Close()
			fmt.Println(tuple.GetAsString())
		}
		fmt.Println("Num rows:", queryResult.GetNumberOfRows())
		fmt.Println("Time:", queryResult.GetCompilingTime(), queryResult.GetExecutionTime())
	}

	result, err := conn.Query("MATCH (a:User)-[e:Follows]->(b:User) RETURN a.name, e.since, b.name")
	if err != nil {
		fmt.Println(err)
	}
	defer result.Close()
	for result.HasNext() {
		tuple, err := result.Next()
		if err != nil {
			fmt.Println(err)
			break
		}
		fmt.Println(tuple.GetAsSlice())
		fmt.Println(tuple.GetAsMap())
		defer tuple.Close()
	}

	result, err = conn.Query("MATCH (a:User)-[e:Follows]->(b:User) RETURN *")
	if err != nil {
		fmt.Println(err)
	}
	defer result.Close()
	for result.HasNext() {
		tuple, err := result.Next()
		if err != nil {
			fmt.Println(err)
			break
		}
		fmt.Println(tuple.GetAsMap())
		defer tuple.Close()
	}

	result, err = conn.Query("RETURN [[1, 2, 3], [4, 5, 6]]")
	if err != nil {
		fmt.Println(err)
	}
	defer result.Close()
	for result.HasNext() {
		tuple, err := result.Next()
		if err != nil {
			fmt.Println(err)
			break
		}
		fmt.Println(tuple.GetAsMap())
		defer tuple.Close()
	}

	result, err = conn.Query("RETURN array_value(1,2,3,4,5)")
	if err != nil {
		fmt.Println(err)
	}
	defer result.Close()
	for result.HasNext() {
		tuple, err := result.Next()
		if err != nil {
			fmt.Println(err)
			break
		}
		fmt.Println(tuple.GetAsMap())
		defer tuple.Close()
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

	preparedStatement, err = conn.Prepare("RETURN $1")
	if err != nil {
		fmt.Println("Prepare error:", err, preparedStatement)
	}
	defer preparedStatement.Close()
	dateDiff := time.Date(2021, time.January, 1, 0, 0, 0, 0, time.UTC).Sub(time.Date(2001, time.January, 1, 0, 0, 0, 0, time.UTC))
	fmt.Println("dateDiff:", dateDiff)

	args = map[string]interface{}{
		"1": dateDiff,
	}
	queryResult, err = conn.Execute(preparedStatement, args)
	if err != nil {
		fmt.Println(err)
	}
	defer queryResult.Close()
	fmt.Println(queryResult.ToString())

	queryResult, err = conn.Query("RETURN CAST('184467440737095516158', 'INT128')")
	if err != nil {
		fmt.Println(err)
	}
	defer queryResult.Close()
	s, _ := queryResult.Next()
	v, _ := s.GetAsSlice()
	fmt.Println(v)

	queryResult, err = conn.Query("RETURN CAST('-184467440737095516158', 'INT128')")
	if err != nil {
		fmt.Println(err)
	}
	defer queryResult.Close()
	s, _ = queryResult.Next()
	v, _ = s.GetAsSlice()
	fmt.Println(v)
	fmt.Println(reflect.TypeOf(v[0]))

	queryResult, err = conn.Query("RETURN CAST('123e4567-e89b-12d3-a456-426614174000', 'UUID')")
	if err != nil {
		fmt.Println(err)
	}
	defer queryResult.Close()
	s, _ = queryResult.Next()
	v, _ = s.GetAsSlice()
	fmt.Println(v)
	fmt.Println(reflect.TypeOf(v[0]))

	// queryResult, err = conn.Query("MATCH (a:T_l) RETURN a.val ORDER BY a.id;")
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// defer queryResult.Close()
	// for queryResult.HasNext() {
	// 	s, _ := queryResult.Next()
	// 	v, _ := s.GetAsSlice()
	// 	fmt.Println(v)
	// 	fmt.Println(reflect.TypeOf(v[0]))
	// }

	queryResult, err = conn.Query("MATCH p=()-[]->() RETURN p")
	if err != nil {
		fmt.Println(err)
	}
	defer queryResult.Close()
	for queryResult.HasNext() {
		s, _ := queryResult.Next()
		v, _ := s.GetAsSlice()
		fmt.Println(v)
		fmt.Println(reflect.TypeOf(v[0]))
	}

	queryResult, err = conn.Query("RETURN 1; RETURN 2;")
	if err != nil {
		fmt.Println(err)
	}
	defer queryResult.Close()
	s, _ = queryResult.Next()
	v, _ = s.GetAsSlice()
	fmt.Println(v)
	println(queryResult.HasNextQueryResult())
	queryResult, err = queryResult.NextQueryResult()
	if err != nil {
		fmt.Println(err)
	}
	defer queryResult.Close()
	s, _ = queryResult.Next()
	v, _ = s.GetAsSlice()
	fmt.Println(v)
}
