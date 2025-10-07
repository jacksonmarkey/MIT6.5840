package lock

import (
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
	// Initially I initialized the lock here, but then I changed Acquire
	// to handle the case where we first acquire the lock instead
	// _, _, err := ck.Get(lk.lockID)
	// if err == rpc.ErrNoKey {
	// 	for {
	// 		err = ck.Put(lk.lockID, UnlockedState, 0)
	// 		if err == rpc.OK || err == rpc.ErrVersion {
	// 			break
	// 		}
	// 		// Keep looping until we get
	// 	}
	// }
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		value, version, getErr := lk.ck.Get(lk.lockID)
		var putErr rpc.Err
		switch getErr {
		case rpc.ErrNoKey:
			putErr = lk.ck.Put(lk.lockID, lk.clientID, 0)
		case rpc.OK:
			if value == lk.clientID {
				// We already have the lock
				return
			}
			if value == UnlockedState {
				putErr = lk.ck.Put(lk.lockID, lk.clientID, version)
			}
		}
		if putErr == rpc.OK {
			return
		}
	}

}

func (lk *Lock) Release() {
	// Your code here
	for {
		value, version, getErr := lk.ck.Get(lk.lockID)
		switch getErr {
		case rpc.ErrNoKey:
			// Lock has never been locked
			return
		case rpc.OK:
			if value != lk.clientID {
				// If locked to another client already, assume the following:
				// 1. We successfully unlocked previously in the loop
				// 2. The network lost our confirmation of that successful unlock
				// 3. By the time we sent our current Get request, someone else
				// 		acquired the lock
				// In this case, we should return
				return
			}
			// We do currently have the lock, so we need to release it
			putErr := lk.ck.Put(lk.lockID, UnlockedState, version)
			if putErr == rpc.OK {
				return
			}
			// If we don't get an OK, it doesn't matter what specific error we got.
			// To handle them all, we need to check the state again,
			// so we go back to the start of the loop.
		}
	}
}
