package pools

import "sync"

type ring struct {
	mutex  sync.Mutex // protect this shared structure
	cnt, i int
	data   []Resource
}

func (rb *ring) Size() (cnt int) {
	rb.mutex.Lock()
	cnt = rb.cnt
	rb.mutex.Unlock()
	return
}

func (rb *ring) Empty() (b bool) {
	rb.mutex.Lock()
	b = (rb.cnt == 0)
	rb.mutex.Unlock()
	return
}

func (rb *ring) Peek() (r Resource) {
	rb.mutex.Lock()
	r = rb.peekWithLock()
	rb.mutex.Unlock()
	return
}

func (rb *ring) Enqueue(x Resource) {
	rb.mutex.Lock()
	if rb.cnt >= len(rb.data) {
		rb.growWithLock(2*rb.cnt + 1)
	}
	rb.data[(rb.i+rb.cnt)%len(rb.data)] = x
	rb.cnt++
	rb.mutex.Unlock()
}

func (rb *ring) Dequeue() (x Resource) {
	rb.mutex.Lock()
	x = rb.peekWithLock()
	rb.cnt, rb.i = rb.cnt-1, (rb.i+1)%len(rb.data)
	rb.mutex.Unlock()
	return
}

func (rb *ring) growWithLock(newSize int) {
	// mutex must be locked by caller
	newData := make([]Resource, newSize)

	n := copy(newData, rb.data[rb.i:])
	copy(newData[n:], rb.data[:rb.cnt-n])

	rb.i = 0
	rb.data = newData
}
func (rb *ring) peekWithLock() (r Resource) {
	// mutex must be locked by caller
	r = rb.data[rb.i]
	return
}
