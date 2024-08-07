package kuzu

// #include "kuzu.h"
// #include <stdlib.h>
import "C"

import (
	"math"
	"time"
)

func unixEpoch() time.Time {
	return time.Unix(0, 0)
}

func timeToKuzuDate(inputTime time.Time) C.kuzu_date_t {
	diff := inputTime.Sub(unixEpoch())
	diffDays := math.Floor(diff.Hours() / 24)
	cKuzuDate := C.kuzu_date_t{}
	cKuzuDate.days = C.int32_t(diffDays)
	return cKuzuDate
}

func kuzuDateToTime(cKuzuDate C.kuzu_date_t) time.Time {
	diff := time.Duration(cKuzuDate.days) * 24 * time.Hour
	return unixEpoch().Add(diff)
}

func timeToKuzuTimestamp(inputTime time.Time) C.kuzu_timestamp_t {
	nanoseconds := inputTime.UnixNano()
	microseconds := nanoseconds / 1000
	cKuzuTime := C.kuzu_timestamp_t{}
	cKuzuTime.value = C.int64_t(microseconds)
	return cKuzuTime
}

func timeToKuzuTimestampNs(inputTime time.Time) C.kuzu_timestamp_ns_t {
	nanoseconds := inputTime.UnixNano()
	cKuzuTime := C.kuzu_timestamp_ns_t{}
	cKuzuTime.value = C.int64_t(nanoseconds)
	return cKuzuTime
}

func timeHasNanoseconds(inputTime time.Time) bool {
	return inputTime.Nanosecond() != 0
}
