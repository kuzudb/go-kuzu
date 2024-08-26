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
	return unixEpoch().UTC().Add(diff)
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

func durationToKuzuInterval(inputDuration time.Duration) C.kuzu_interval_t {
	microseconds := inputDuration.Microseconds()

	cKuzuInterval := C.kuzu_interval_t{}
	cKuzuInterval.micros = C.int64_t(microseconds)
	return cKuzuInterval
}

func kuzuIntervalToDuration(cKuzuInterval C.kuzu_interval_t) time.Duration {
	days := cKuzuInterval.days
	months := cKuzuInterval.months
	microseconds := cKuzuInterval.micros
	totalDays := int64(days) + int64(months)*30
	totalSeconds := totalDays * 24 * 60 * 60
	totalMicroseconds := totalSeconds*1000000 + int64(microseconds)
	totalNanoseconds := totalMicroseconds * 1000
	return time.Duration(totalNanoseconds)
}
