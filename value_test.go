package kuzu

import (
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
