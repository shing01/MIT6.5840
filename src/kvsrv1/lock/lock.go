package lock

import (
	"6.5840/kvtest1"
	"6.5840/kvsrv1/rpc"
	"time"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck   kvtest.IKVClerk
	// You may add code here
	name string
	id   string 
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:   ck,
		name: l,
		id: kvtest.RandValue(8),
	}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	myToken := lk.id
	for {
		// get lock's state
		val, ver, err := lk.ck.Get(lk.name)

		// check lock is occupied or not
		if err == rpc.ErrNoKey || (err == rpc.OK && val == "") {
			putVer := ver
			if err == rpc.ErrNoKey {
				putVer = 0
			}

			err2 := lk.ck.Put(lk.name, myToken, putVer) // locked
			if err2 == rpc.OK {
				return
			}
			if err2 == rpc.ErrMaybe {
				checkVal, _, checkErr := lk.ck.Get(lk.name)
				if checkErr == rpc.OK && checkVal == myToken {
					return
				}
			}
		}
		time.Sleep(10*time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		val, ver, err := lk.ck.Get(lk.name)

		if err == rpc.ErrNoKey || (err == rpc.OK && val == "") {
			return
		}
		if err == rpc.OK && val != lk.id {
			return
		}

		err2 := lk.ck.Put(lk.name, "", ver)
		if err2 == rpc.OK {
			return
		}
		if err2 == rpc.ErrMaybe {
			checkVal, _, checkErr := lk.ck.Get(lk.name)
			if checkErr == rpc.ErrNoKey || (checkErr == rpc.OK && checkVal != lk.id) {
				return
			}
		}
		time.Sleep(10*time.Millisecond)
	}
}
