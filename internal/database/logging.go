package database

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/julieqiu/derrors"
	"github.com/julieqiu/dlog"
)

// QueryLoggingDisabled stops logging of queries when true.
// For use in tests only: not concurrency-safe.
var QueryLoggingDisabled bool

var queryCounter int64 // atomic: per-process counter for unique query IDs

type queryEndLogEntry struct {
	ID              string
	Query           string
	Args            string
	DurationSeconds float64
	Error           string `json:",omitempty"`
}

func logQuery(ctx context.Context, query string, args []interface{}, instanceID string, retryable bool) func(*error) {
	if QueryLoggingDisabled {
		return func(*error) {}
	}
	const maxlen = 300 // maximum length of displayed query

	// To make the query more compact and readable, replace newlines with spaces
	// and collapse adjacent whitespace.
	var r []rune
	for _, c := range query {
		if c == '\n' {
			c = ' '
		}
		if len(r) == 0 || !unicode.IsSpace(r[len(r)-1]) || !unicode.IsSpace(c) {
			r = append(r, c)
		}
	}
	query = string(r)
	if len(query) > maxlen {
		query = query[:maxlen] + "..."
	}

	uid := generateLoggingID(instanceID)

	// Construct a short string of the args.
	const (
		maxArgs   = 20
		maxArgLen = 50
	)
	var argStrings []string
	for i := 0; i < len(args) && i < maxArgs; i++ {
		s := fmt.Sprint(args[i])
		if len(s) > maxArgLen {
			s = s[:maxArgLen] + "..."
		}
		argStrings = append(argStrings, s)
	}
	if len(args) > maxArgs {
		argStrings = append(argStrings, "...")
	}
	argString := strings.Join(argStrings, ", ")

	dlog.Debugf(ctx, "%s %s args=%s", uid, query, argString)
	start := time.Now()
	return func(errp *error) {
		dur := time.Since(start)
		if errp == nil { // happens with queryRow
			dlog.Debugf(ctx, "%s done", uid)
		} else {
			derrors.Wrap(errp, "DB running query %s", uid)
			entry := queryEndLogEntry{
				ID:              uid,
				Query:           query,
				Args:            argString,
				DurationSeconds: dur.Seconds(),
			}
			if *errp == nil {
				dlog.Debug(ctx, entry)
			} else {
				entry.Error = (*errp).Error()
				// There are many places in our logs when a query will be
				// canceled, because all unfinished search queries for a given
				// request are canceled:
				//
				// We don't want to log these as errors, because it makes the logs
				// very noisy. Based on
				// https://github.com/lib/pq/issues/577#issuecomment-298341053
				// it seems that ctx.Err() could return nil because this error
				// is coming from postgres. github.com/lib/pq currently handles
				// errors like these in their tests by hardcoding the string:
				// https://github.com/lib/pq/blob/e53edc9b26000fec4c4e357122d56b0f66ace6ea/go18_test.go#L89
				logf := dlog.Error
				if errors.Is(ctx.Err(), context.Canceled) ||
					strings.Contains(entry.Error, "pq: canceling statement due to user request") {
					logf = dlog.Debug
				}
				// If the transaction is retryable and this is a serialization error,
				// then it's not really an error at all. Log it as debug, so if
				// we get a "failed due to max retries" error, we can find
				// these easily.
				if retryable && isSerializationFailure(*errp) {
					logf = dlog.Debug
				}
				logf(ctx, entry)
			}
		}
	}
}

func (db *DB) logTransaction(ctx context.Context) func(*error) {
	if QueryLoggingDisabled {
		return func(*error) {}
	}
	uid := generateLoggingID(db.instanceID)
	dlog.Debugf(ctx, "%s transaction (isolation %s) started", uid, db.opts.Isolation)
	start := time.Now()
	return func(errp *error) {
		dlog.Debugf(ctx, "%s transaction (isolation %s) finished in %s with error %v",
			uid, db.opts.Isolation, time.Since(start), *errp)
	}
}

func generateLoggingID(instanceID string) string {
	if instanceID == "" {
		instanceID = "local"
	} else if len(instanceID) > 8 {
		// App Engine instance IDs are long strings. The low-order part seems
		// quite random, so shortening the ID will still likely result in
		// something unique.
		instanceID = instanceID[len(instanceID)-4:]
	}
	n := atomic.AddInt64(&queryCounter, 1)
	return fmt.Sprintf("%s-%d", instanceID, n)
}
