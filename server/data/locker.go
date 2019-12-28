package data

import (
	"sync"
)

var GlobalLocker = &locker{
	locks: map[*Row]*transactionMutexTuple{},
}

type locker struct {
	locks map[*Row]*transactionMutexTuple
	mu    sync.Mutex
}

type transactionMutexTuple struct {
	transaction    *Transaction
	lockAndWaiting int
	mu             *sync.Mutex
}

func (l *locker) Lock(r *Row, trx *Transaction) {
	tuple := l.getOrCreateMutexTuple(r)

	if tuple.transaction == trx {
		return
	}

	tuple.mu.Lock()
	tuple.transaction = trx
}

func (l *locker) getOrCreateMutexTuple(r *Row) *transactionMutexTuple {
	mu.Lock()
	defer mu.Unlock()

	if tuple, ok := l.locks[r]; ok {
		tuple.lockAndWaiting += 1
		return tuple
	}

	tuple := &transactionMutexTuple{
		mu:             &sync.Mutex{},
		lockAndWaiting: 1,
	}
	l.locks[r] = tuple
	return tuple
}

func (l *locker) Unlock(r *Row, trx *Transaction) {
	tuple := l.locks[r]
	if tuple == nil {
		panic("tried to unlock not locked row")
	}
	tuple.mu.Unlock()

	mu.Lock()
	defer mu.Unlock()
	if tuple.transaction != trx {
		panic("tried to unlock different transaction's lock")
	}
	tuple.lockAndWaiting -= 1
	if tuple.lockAndWaiting <= 0 {
		delete(l.locks, r)
	}
}

func (l *locker) HasLock(r *Row, trx *Transaction) bool {
	if r, ok := l.locks[r]; ok {
		return r.transaction == trx
	}
	return false
}
