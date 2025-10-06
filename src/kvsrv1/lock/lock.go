package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

const (
	UnlockedState = "unlocked"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockID   string
	clientID string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:       ck,
		lockID:   l,
		clientID: kvtest.RandValue(8),
	}
	// You may add code here
	_, _, err := ck.Get(lk.lockID)
	if err == rpc.ErrNoKey {
		ck.Put(lk.lockID, UnlockedState, 0)
	}
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		value, version, err := lk.ck.Get(lk.lockID)
		if err != rpc.OK {
			continue
		}
		if value != UnlockedState {
			// Special error if it's already locked to this specific client?
			time.Sleep(time.Second)
			continue
		}
		err = lk.ck.Put(lk.lockID, lk.clientID, version)
		if err != rpc.OK {
			continue
		}
		return
	}

}

func (lk *Lock) Release() {
	// Your code here
	for {
		value, version, err := lk.ck.Get(lk.lockID)
		if err != rpc.OK {
			continue
		}
		if value != lk.clientID {
			// Error if it's locked to another client?
			continue
		}
		err = lk.ck.Put(lk.lockID, UnlockedState, version)
		if err != rpc.OK {
			continue
		}
		return
	}
}
