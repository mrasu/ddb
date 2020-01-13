package data

import (
	"testing"
	"time"

	"github.com/mrasu/ddb/thelper"
)

func TestLocker_Lock(t *testing.T) {
	locker := &locker{locks: map[*Row]*transactionMutexTuple{}}

	table := newEmtpyTable("hello")
	r1 := newEmptyRow(table)
	r2 := newEmptyRow(table)
	r3 := newEmptyRow(table)

	trx1 := StartNewTransaction()
	trx2 := StartNewTransaction()
	locker.Lock(r1, trx1)
	locker.Lock(r2, trx1)
	locker.Lock(r3, trx2)
	// finish without deadlock
}

func TestLocker_HasLock(t *testing.T) {
	locker := &locker{locks: map[*Row]*transactionMutexTuple{}}
	r := newEmptyRow(newEmtpyTable("hello"))

	trx1 := StartNewTransaction()
	trx2 := StartNewTransaction()
	thelper.AssertBool(t, "Already locked", false, locker.HasLock(r, trx1))

	locker.Lock(r, trx1)

	thelper.AssertBool(t, "Not locked", true, locker.HasLock(r, trx1))
	thelper.AssertBool(t, "Other transaction has lock", false, locker.HasLock(r, trx2))
}

func TestLocker_Unlock_AllowOtherLock(t *testing.T) {
	locker := &locker{locks: map[*Row]*transactionMutexTuple{}}
	r := newEmptyRow(newEmtpyTable("hello"))

	trx1 := StartNewTransaction()
	trx2 := StartNewTransaction()
	locker.Lock(r, trx1)

	lockIndicator := 0
	go func() {
		lockIndicator = 1
		locker.Lock(r, trx2)
		lockIndicator = 2
	}()
	time.Sleep(1 * time.Millisecond)
	thelper.AssertInt(t, "trx2 doesn't start lock", 1, lockIndicator)

	locker.Unlock(r, trx1)
	time.Sleep(1 * time.Millisecond)
	thelper.AssertInt(t, "trx2 acquire lock", 2, lockIndicator)
}
