package kuzu

// #include "kuzu.h"
// #include <stdlib.h>
import "C"

import (
	"math"
	"time"
)

// unixEpoch returns the Unix epoch time.
func unixEpoch() time.Time {
	return time.Unix(0, 0)
}

// timeToKuzuDate converts a time.Time to a kuzu_date_t.
func timeToKuzuDate(inputTime time.Time) C.kuzu_date_t {
	diff := inputTime.Sub(unixEpoch())
	diffDays := math.Floor(diff.Hours() / 24)
	cKuzuDate := C.kuzu_date_t{}
	cKuzuDate.days = C.int32_t(diffDays)
	return cKuzuDate
}

// kuzuDateToTime converts a kuzu_date_t to a time.Time in UTC.
func kuzuDateToTime(cKuzuDate C.kuzu_date_t) time.Time {
	diff := time.Duration(cKuzuDate.days) * 24 * time.Hour
	return unixEpoch().UTC().Add(diff)
}

// timeToKuzuTimestamp converts a time.Time to a kuzu_timestamp_t.
func timeToKuzuTimestamp(inputTime time.Time) C.kuzu_timestamp_t {
	nanoseconds := inputTime.UnixNano()
	microseconds := nanoseconds / 1000
	cKuzuTime := C.kuzu_timestamp_t{}
	cKuzuTime.value = C.int64_t(microseconds)
	return cKuzuTime
}

// timeToKuzuTimestampNs converts a time.Time to a kuzu_timestamp_ns_t.
func timeToKuzuTimestampNs(inputTime time.Time) C.kuzu_timestamp_ns_t {
	nanoseconds := inputTime.UnixNano()
	cKuzuTime := C.kuzu_timestamp_ns_t{}
	cKuzuTime.value = C.int64_t(nanoseconds)
	return cKuzuTime
}

// timeHasNanoseconds returns true if the time.Time has non-zero nanoseconds.
func timeHasNanoseconds(inputTime time.Time) bool {
	return inputTime.Nanosecond() != 0
}

// durationToKuzuInterval converts a time.Duration to a kuzu_interval_t.
func durationToKuzuInterval(inputDuration time.Duration) C.kuzu_interval_t {
	microseconds := inputDuration.Microseconds()

	cKuzuInterval := C.kuzu_interval_t{}
	cKuzuInterval.micros = C.int64_t(microseconds)
	return cKuzuInterval
}

// kuzuIntervalToDuration converts a kuzu_interval_t to a time.Duration.
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
