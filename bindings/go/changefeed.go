package latticedb

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// ChangefeedStatus is the changefeed's retention policy and the events it
// holds.
type ChangefeedStatus struct {
	// Retention is the policy in force, in the form
	// OpenOptions.ChangefeedRetention takes: "all", "off", "last:<events>" or
	// "age:<duration>".
	Retention string
	// OldestSequence is the oldest event held, 0 when the changefeed holds
	// none.
	OldestSequence uint64
	// LastSequence is the last sequence handed to an event, 0 if none ever
	// was.
	LastSequence uint64
}

// ErrChangesTrimmed matches (with errors.Is) the error Changes returns when
// the changefeed no longer holds the events after the position read from:
// its retention policy, or a trim, dropped them, so the reader has missed
// changes. Reading from afterSequence 0 starts at the oldest event held.
// Like every *Error, it matches by code, here ErrorNotFound.
var ErrChangesTrimmed = &Error{Code: ErrorNotFound, Message: "changefeed events dropped"}

// lattice_changefeed_retention values.
const (
	changefeedRetentionDefault uint8 = 0
	changefeedRetentionAll     uint8 = 1
	changefeedRetentionLast    uint8 = 2
	changefeedRetentionAge     uint8 = 3
	changefeedRetentionOff     uint8 = 4
)

// Duration suffixes of an "age:" policy, "ms" first so it is not read as "s".
var durationUnits = []struct {
	suffix string
	ms     uint64
}{
	{"ms", 1},
	{"s", 1000},
	{"m", 60 * 1000},
	{"h", 60 * 60 * 1000},
	{"d", 24 * 60 * 60 * 1000},
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// changefeedRetentionCode turns a policy such as "last:1000" into the C API's
// mode and value.
func changefeedRetentionCode(policy string) (uint8, uint64, error) {
	switch {
	case policy == "":
		return changefeedRetentionDefault, 0, nil
	case strings.EqualFold(policy, "all"):
		return changefeedRetentionAll, 0, nil
	case strings.EqualFold(policy, "off"):
		return changefeedRetentionOff, 0, nil
	case strings.HasPrefix(policy, "last:"):
		digits := policy[len("last:"):]
		if allDigits(digits) {
			if n, err := strconv.ParseUint(digits, 10, 64); err == nil && n > 0 {
				return changefeedRetentionLast, n, nil
			}
		}
	case strings.HasPrefix(policy, "age:"):
		text := policy[len("age:"):]
		for _, unit := range durationUnits {
			number := strings.TrimSuffix(text, unit.suffix)
			if number == text || !allDigits(number) {
				continue
			}
			n, err := strconv.ParseUint(number, 10, 64)
			if err == nil && n > 0 && n <= math.MaxUint64/unit.ms {
				return changefeedRetentionAge, n * unit.ms, nil
			}
			break
		}
	}
	return 0, 0, &Error{Code: ErrorInvalidArg, Message: fmt.Sprintf(
		"ChangefeedRetention must be \"all\", \"off\", \"last:<events>\" or \"age:<duration>\" "+
			"(such as \"age:7d\", \"age:12h\", \"age:500ms\"), not %q", policy)}
}

func changefeedRetentionText(mode uint8, value uint64) string {
	switch mode {
	case changefeedRetentionOff:
		return "off"
	case changefeedRetentionLast:
		return fmt.Sprintf("last:%d", value)
	case changefeedRetentionAge:
		for i := len(durationUnits) - 1; i >= 1; i-- {
			if value%durationUnits[i].ms == 0 {
				return fmt.Sprintf("age:%d%s", value/durationUnits[i].ms, durationUnits[i].suffix)
			}
		}
		return fmt.Sprintf("age:%dms", value)
	default:
		return "all"
	}
}
