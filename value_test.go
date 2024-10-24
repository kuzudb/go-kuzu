package kuzu

import (
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBool(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("MATCH (a:person) WHERE a.ID = 0 RETURN a.isStudent;")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.True(t, value.(bool))
	res.Close()
}

func TestInt64(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("MATCH (a:person) WHERE a.ID = 0 RETURN a.age;")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, int64(35), value)
	res.Close()
}

func TestInt32(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("RETURN CAST (170, \"INT32\")")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, int32(170), value)
	res.Close()
}

func TestInt16(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("RETURN CAST (888, \"INT16\")")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, int16(888), value)
	res.Close()
}

func TestInt8(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("MATCH (a:person) -[r:studyAt]-> (b:organisation) WHERE r.length = 5 RETURN r.level;")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, int8(5), value)
	res.Close()
}

func TestUint64(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("MATCH (a:person) -[r:studyAt]-> (b:organisation) WHERE r.length = 5 RETURN r.code;")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, uint64(9223372036854775808), value)
	res.Close()
}
func TestUint32(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("MATCH (a:person) -[r:studyAt]-> (b:organisation) WHERE r.length = 5 RETURN r.temperature;")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, uint32(32800), value)
	res.Close()
}
func TestUint16(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("MATCH (a:person) -[r:studyAt]-> (b:organisation) WHERE r.length = 5 RETURN r.ulength;")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, uint16(33768), value)
	res.Close()
}
func TestUint8(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("MATCH (a:person) -[r:studyAt]-> (b:organisation) WHERE r.length = 5 RETURN r.ulevel;")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, uint8(250), value)
	res.Close()
}

func TestInt128(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("RETURN CAST (18446744073709551610, \"INT128\")")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	int128Value := value.(*big.Int)
	assert.Equal(t, "18446744073709551610", int128Value.String())

	res, error = conn.Query("RETURN CAST (-18446744073709551610, \"INT128\")")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ = res.Next()
	value, _ = next.GetValue(0)
	int128Value = value.(*big.Int)
	assert.Equal(t, "-18446744073709551610", int128Value.String())
	res.Close()
}

func TestSerial(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("MATCH (a:moviesSerial) WHERE a.ID = 2 RETURN a.ID;")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, int64(2), value)
	res.Close()
}

func TestDouble(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("MATCH (a:person) WHERE a.ID = 0 RETURN a.eyeSight;")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.InDelta(t, float64(5.0), value, floatEpsilon)
	res.Close()
}

func TestFloat(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("RETURN CAST (1.75, \"FLOAT\")")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.InDelta(t, float32(1.75), value, floatEpsilon)
	res.Close()
}

func TestString(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("MATCH (a:person) WHERE a.ID = 0 RETURN a.fName;")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, "Alice", value)
	res.Close()
}

func TestBlob(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("RETURN BLOB('\\\\xAA\\\\xBB\\\\xCD\\\\x1A')")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, byte(0xAA), value.([]byte)[0])
	assert.Equal(t, byte(0xBB), value.([]byte)[1])
	assert.Equal(t, byte(0xCD), value.([]byte)[2])
	assert.Equal(t, byte(0x1A), value.([]byte)[3])
	res.Close()
}

func TestDate(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("RETURN DATE('1985-01-01')")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	time := value.(time.Time)
	assert.Equal(t, 1985, time.Year())
	assert.Equal(t, 1, int(time.Month()))
	assert.Equal(t, 1, time.Day())
	assert.Equal(t, 0, time.Hour())
	res.Close()
}

func TestTimestamp(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("RETURN TIMESTAMP('1970-01-01T00:00:00Z')")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	time := value.(time.Time)
	time = time.UTC()
	assert.Equal(t, 1970, time.Year())
	assert.Equal(t, 1, int(time.Month()))
	assert.Equal(t, 1, time.Day())
	assert.Equal(t, 0, time.Hour())
	assert.Equal(t, 0, time.Minute())
	assert.Equal(t, 0, time.Second())
	res.Close()
}

func TestTimestampNs(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	preparedStatement, error := conn.Prepare("RETURN $1")
	assert.Nil(t, error)
	params := map[string]interface{}{
		"1": time.Date(1970, 1, 1, 0, 0, 0, 1, time.UTC),
	}
	res, error := conn.Execute(preparedStatement, params)
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	time := value.(time.Time)
	time = time.UTC()
	assert.Equal(t, 1970, time.Year())
	assert.Equal(t, 1, int(time.Month()))
	assert.Equal(t, 1, time.Day())
	assert.Equal(t, 0, time.Hour())
	assert.Equal(t, 0, time.Minute())
	assert.Equal(t, 0, time.Second())
	assert.Equal(t, 1, time.Nanosecond())
	res.Close()
}

func TestTimestampMs(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	preparedStatement, error := conn.Prepare("RETURN CAST ($1, \"TIMESTAMP_MS\")")
	assert.Nil(t, error)
	inputTime, error := time.Parse(time.RFC3339, "2024-08-29T10:03:05Z")
	// Add 3 milliseconds
	duration, err := time.ParseDuration("3ms")
	if err != nil {
		t.Fatal(err)
	}
	inputTime = inputTime.Add(duration)
	assert.Nil(t, error)
	params := map[string]interface{}{
		"1": inputTime,
	}
	res, error := conn.Execute(preparedStatement, params)
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, inputTime.Local(), value)
}

func TestTimestampSec(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	preparedStatement, error := conn.Prepare("RETURN CAST ($1, \"TIMESTAMP_SEC\")")
	assert.Nil(t, error)
	inputTime, error := time.Parse(time.RFC3339, "2024-08-29T10:03:05Z")
	assert.Nil(t, error)
	params := map[string]interface{}{
		"1": inputTime,
	}
	res, error := conn.Execute(preparedStatement, params)
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, inputTime.Local(), value)
}

func TestTimestampTz(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	preparedStatement, error := conn.Prepare("RETURN CAST ($1, \"TIMESTAMP_TZ\")")
	assert.Nil(t, error)
	inputTime, error := time.Parse(time.RFC3339, "2024-08-29T10:03:05Z")
	assert.Nil(t, error)
	params := map[string]interface{}{
		"1": inputTime,
	}
	res, error := conn.Execute(preparedStatement, params)
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, inputTime.Local(), value)
}

func TestInterval(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("RETURN INTERVAL(\"3 days\");")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, time.Duration(3*24*time.Hour), value)
}

func TestList(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("RETURN [[1, 2, 3], [4, 5, 6]]")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, []interface{}{int64(1), int64(2), int64(3)}, value.([]interface{})[0])
	assert.Equal(t, []interface{}{int64(4), int64(5), int64(6)}, value.([]interface{})[1])
}

func TestArray(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("RETURN CAST([3, 4, 12, 11], 'INT64[4]')")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, []interface{}{int64(3), int64(4), int64(12), int64(11)}, value)
}

func TestStruct(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("RETURN {name: 'Alice', age: 30}")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.Equal(t, "Alice", value.(map[string]interface{})["name"])
	assert.Equal(t, int64(30), value.(map[string]interface{})["age"])
}

func TestMap(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("MATCH (m:movies) WHERE m.length = 2544 RETURN m.audience")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.InDelta(t, float64(33), value.(map[string]any)["audience1"], floatEpsilon)
}

func TestUnion(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("MATCH (m:movies) WHERE m.length = 2544 RETURN m.grade;")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	assert.InDelta(t, float64(8.989), value.(map[string]any)["credit"], floatEpsilon)
}

func TestNode(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("MATCH (a:person) WHERE a.ID = 0 RETURN a;")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	node := value.(Node)
	assert.Equal(t, "person", node.Label)
	assert.Equal(t, int64(0), node.Properties["ID"])
	assert.Equal(t, "Alice", node.Properties["fName"])
	assert.Equal(t, int64(1), node.Properties["gender"])
	assert.Equal(t, int64(35), node.Properties["age"])
	assert.Equal(t, true, node.Properties["isStudent"])
	assert.Equal(t, false, node.Properties["isWorker"])
	assert.InDelta(t, float64(5.0), node.Properties["eyeSight"], floatEpsilon)
	birthDate := node.Properties["birthdate"].(time.Time)
	assert.Equal(t, 1900, birthDate.Year())
	assert.Equal(t, 1, int(birthDate.Month()))
	assert.Equal(t, 1, birthDate.Day())
	registerTime := node.Properties["registerTime"].(time.Time).UTC()
	assert.Equal(t, 2011, registerTime.Year())
	assert.Equal(t, 8, int(registerTime.Month()))
	assert.Equal(t, 20, registerTime.Day())
	assert.Equal(t, 11, registerTime.Hour())
	assert.Equal(t, 25, registerTime.Minute())
	assert.Equal(t, 30, registerTime.Second())
	lastJobDuration := node.Properties["lastJobDuration"].(time.Duration)
	assert.Equal(t, 1082*24*time.Hour+46920*time.Second, lastJobDuration)
	courseScoresPerTerm := node.Properties["courseScoresPerTerm"].([]interface{})
	assert.Equal(t, 2, len(courseScoresPerTerm))
	assert.Equal(t, []interface{}{int64(10), int64(8)}, courseScoresPerTerm[0].([]interface{}))
	assert.Equal(t, []interface{}{int64(6), int64(7), int64(8)}, courseScoresPerTerm[1].([]interface{}))
	usedNames := node.Properties["usedNames"].([]interface{})
	assert.Equal(t, 1, len(usedNames))
	assert.Equal(t, "Aida", usedNames[0])
	res.Close()
}

func TestRelationship(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("MATCH (p:person)-[r:workAt]->(o:organisation) WHERE p.ID = 5 RETURN p, r, o")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	m, err := next.GetAsMap()
	assert.Nil(t, err)
	rel := m["r"].(Relationship)
	src := m["p"].(Node)
	dst := m["o"].(Node)
	assert.Equal(t, "workAt", rel.Label)
	assert.Equal(t, rel.SourceID, src.ID)
	assert.Equal(t, rel.DestinationID, dst.ID)
	assert.Equal(t, int64(2010), rel.Properties["year"])
}

func TestRecursiveRel(t *testing.T) {
	_, conn := SetupTestDatabase(t)
	res, error := conn.Query("MATCH (a:person)-[e:studyAt*1..1]->(b:organisation) WHERE a.fName = 'Alice' RETURN e;")
	assert.Nil(t, error)
	assert.True(t, res.HasNext())
	next, _ := res.Next()
	value, _ := next.GetValue(0)
	recursiveRel := value.(RecursiveRelationship)
	assert.Equal(t, len(recursiveRel.Nodes), 0)
	assert.Equal(t, len(recursiveRel.Relationships), 1)
	rel := recursiveRel.Relationships[0]
	assert.Equal(t, "studyAt", rel.Label)
	assert.Equal(t, int16(5), rel.Properties["length"])
	assert.Equal(t, int64(2021), rel.Properties["year"])
}
