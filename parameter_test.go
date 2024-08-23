package kuzu

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// func TestArrayBinding(t *testing.T){
// 	_, conn := makeDB(t)
// 	conn.Query("CREATE NODE TABLE node(id STRING, embedding DOUBLE[3], PRIMARY KEY(id))")
// 	q, err := conn.Prepare("CREATE (d:node {id: 'test', embedding: $emb})")
// 	if err != nil{
// 		panic(err)
// 	} 
// 	emb := make(map[string]any)
// 	emb["emb"] = [3]int{3,5,2}
// 	conn.Execute(q, emb)
// 	emb["emb"] = [3]float64{4.3, 5.2, 6.7}
// 	q, err = conn.Prepare(`
//         MATCH (d:node)
//         RETURN d.id, array_cosine_similarity(d.embedding, $emb)
// 						`)
// 	if err != nil{
// 		panic(err)
// 	} 
// 	result, err := conn.Execute(q, emb)
// 	if err != nil{
// 		panic(err)
// 	}
// 	result.HasNext()
// 	// assert.True(t, result.HasNext())
// 	// next, _ := result.Next()
// 	// value, _ := next.GetValue(0)
// 	// assert.Equal(t, value, 0.8922316795174099)
// }

func TestBoolParam(t *testing.T){
	db, conn := makeDB(t)
	m := make(map[string]any)
	m["1"] = false
	m["k"] = false
	stmt, err := conn.Prepare(`Match (a:person) WHERE a.isStudent  = $1 AND a.isWorker = $k RETURN COUNT(*)`)
	if err != nil {
		panic(err)
	}
	result, err := conn.Execute(stmt, m)
	assert.True(t, result.HasNext())
	if err != nil {
		panic(err)
	}
	next, _ := result.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, value, int64(1))
	t.Cleanup(func() {
		db.Close()
		conn.Close()
	})
}

func TestIntParam(t *testing.T){
	db, conn := makeDB(t)
	m := make(map[string]any)
	m["AGE"] = 1
	stmt, err := conn.Prepare(`MATCH (a:person) WHERE a.age < $AGE RETURN COUNT(*)`)
	if err != nil {
		panic(err)
	}
	result, err := conn.Execute(stmt, m)
	assert.True(t, result.HasNext())
	if err != nil {
		panic(err)
	}
	next, _ := result.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, value, int64(0))
	t.Cleanup(func() {
		db.Close()
		conn.Close()
	})
}

func TestStrParam(t *testing.T){
	db, conn := makeDB(t)
	m := make(map[string]any)
	m["S"] = "HH"
	stmt, err := conn.Prepare(`MATCH (a:person) WHERE a.ID = 0 RETURN concat(a.fName, $S);`)
	if err != nil {
		panic(err)
	}
	result, err := conn.Execute(stmt, m)
	assert.True(t, result.HasNext())
	if err != nil {
		panic(err)
	}
	next, _ := result.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, value, "AliceHH")
	t.Cleanup(func() {
		db.Close()
		conn.Close()
	})
}

func TestDoubleParam(t *testing.T){
	db, conn := makeDB(t)
	m := make(map[string]any)
	m["E"] = 5.0 
	stmt, err := conn.Prepare(`MATCH (a:person) WHERE a.eyeSight = $E RETURN COUNT(*)`)
	if err != nil {
		panic(err)
	}
	result, err := conn.Execute(stmt, m)
	assert.True(t, result.HasNext())
	if err != nil {
		panic(err)
	}
	next, _ := result.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, value, int64(2))
	t.Cleanup(func() {
		db.Close()
		conn.Close()
	})
}


