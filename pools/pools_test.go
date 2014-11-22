package pools

import (
	//	"fmt"
	"testing"
	"time"
)

/* Test Closers */
type Thing struct {
	closed bool
}

func (t *Thing) Close() {
	t.closed = true
}

func (t *Thing) IsClosed() bool {
	return t.closed
}

func f() (r Resource, err error) {
	r = &Thing{false}
	return
}

func TestRP(t *testing.T) {
	mi := 2
	mx := 5
	s := make([]Resource, 0)
	rp := NewResourcePool(f, mi, mx)
	go func() {
		if r, err := rp.Acquire(); err != nil {
			t.Log(err)
			t.Fail()
		} else {
			time.Sleep(2 * time.Second)
			rp.Release(r)
		}
	}()
	for i := 0; i < mx; i++ {
		if r, err := rp.Acquire(); err != nil {
			t.Log(err)
			t.Fail()
		} else {
			s = append(s, r)
		}
	}
	c := 0
	for r := range s {
		c += 1
		rp.Release(r)
	}
	if c != mx {
		t.Log("Acquired count is unexpected in return:", c)
		t.Fail()
	}
}

/* Test Non-Closer */
type NCThing struct {
}

func ncf() (r Resource, err error) {
	r = &NCThing{}
	return
}

func TestNCRP(t *testing.T) {
	rp := NewResourcePool(ncf, 10, 20)
	if r, err := rp.Acquire(); err != nil {
		rp.Release(r)
	}
}
