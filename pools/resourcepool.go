package pools

import (
	"errors"
	"sync"
	//"fmt"
)

type closer interface {
	Close()
	IsClosed() bool
}
type Resource interface {
}

type Factory func() (Resource, error)

type ResourcePool struct {
	mutex         sync.Mutex
	factory       Factory
	idleResources ring
	idleCapacity  int
	maxResources  int
	numResources  int

	acqchan chan acquireMessage
	rchan   chan releaseMessage
	cchan   chan closeMessage

	activeWaits []acquireMessage
}

func NewSourcelessPool() (rp *ResourcePool) {
	return NewResourcePool(func() (Resource, error) { return nil, nil }, -1, 0)
}

func NewResourcePool(factory Factory, idleCapacity, maxResources int) (rp *ResourcePool) {
	rp = &ResourcePool{
		factory:      factory,
		idleCapacity: idleCapacity,
		maxResources: maxResources,

		acqchan: make(chan acquireMessage),
		rchan:   make(chan releaseMessage, 1),
		cchan:   make(chan closeMessage, 1),
	}

	go rp.mux()

	return
}

type releaseMessage struct {
	r Resource
}

type acquireMessage struct {
	rch chan Resource
	ech chan error
}

type closeMessage struct {
}

type namedRing struct {
	mutex sync.Mutex
	ring
	Name string
	Cnt  int
}

var acqRing namedRing
var relRing namedRing
var clsRing namedRing

func init() {
	// preallocate messages
	numClsMsgPrealloc := 1
	numAcqMsgPrealloc := 128
	numRelMsgPrealloc := 128
	cMsgs := make([]closeMessage, numClsMsgPrealloc)
	rMsgs := make([]releaseMessage, numRelMsgPrealloc)
	aMsgs := make([]acquireMessage, numAcqMsgPrealloc)
	for _, m := range aMsgs {
		m.rch = make(chan Resource)
		m.ech = make(chan error)
		acqRing.Enqueue(m)
	}
	acqRing.Name = "acqRing"
	for m := range rMsgs {
		relRing.Enqueue(m)
	}
	relRing.Name = "relRing"
	for m := range cMsgs {
		clsRing.Enqueue(m)
	}
	clsRing.Name = "clsRing"

}
func (r *namedRing) get() Resource {
	//fmt.Println("<get", r.Name)
	if r.Empty() {
		return nil
	}
	r.mutex.Lock()
	r.Cnt -= 1
	r.mutex.Unlock()
	//fmt.Println("<get", r.Name, "cnt=", r.Cnt)
	return r.Dequeue()
}
func (r *namedRing) put(m Resource) {
	//fmt.Println(">put", r.Name)
	r.mutex.Lock()
	r.Cnt += 1
	r.mutex.Unlock()
	//fmt.Println(">put", r.Name, "cnt=", r.Cnt)
	r.Enqueue(m)
}

func (rp *ResourcePool) mux() {
loop:
	for {
		select {
		case acq := <-rp.acqchan:
			//fmt.Println("mux acquiring one")
			rp.acquire(acq)
			acqRing.put(acq)
		case rel := <-rp.rchan:
			//fmt.Println("mux releasing one")
			if len(rp.activeWaits) != 0 {
				cr, isCloser := rel.r.(closer)
				// someone is waiting - give them the resource if we can
				if !isCloser || !cr.IsClosed() {
					//fmt.Println("mux returning one to waiter")
					rp.activeWaits[0].rch <- rel.r
				} else {
					// if we can't, discard the released resource and create a new one
					//fmt.Println("mux creating a new one")
					r, err := rp.factory()
					if err != nil {
						// reflect the smaller number of existant resources
						rp.mutex.Lock()
						rp.numResources--
						rp.mutex.Unlock()
						rp.activeWaits[0].ech <- err
					} else {
						//fmt.Println("mux returning one to queue")
						rp.activeWaits[0].rch <- r
					}
				}
				rp.activeWaits = rp.activeWaits[1:]
			} else {
				// if no one is waiting, release it for idling or closing
				//fmt.Println("mux is releasing one")
				rp.release(rel.r)
			}
			relRing.put(rel)

		case cm := <-rp.cchan:
			clsRing.put(cm)
			//fmt.Println("mux is being closed")
			break loop
		}
	}
	for !rp.idleResources.Empty() {
		//fmt.Println("mux is closing resources")
		r := rp.idleResources.Dequeue()
		cr, isCloser := r.(closer)
		if isCloser {
			cr.Close()
		}
	}
	for _, aw := range rp.activeWaits {
		//fmt.Println("mux is providing errors to waiters")
		aw.ech <- errors.New("Resource pool closed")
	}
}

func (rp *ResourcePool) acquire(acq acquireMessage) {
	nr := 0
	for !rp.idleResources.Empty() {
		r := rp.idleResources.Dequeue()
		cr, isCloser := r.(closer)
		if !isCloser || !cr.IsClosed() {
			//fmt.Println("acquire providing one")
			acq.rch <- r
			return
		}
		// discard closed resources
		rp.mutex.Lock()
		rp.numResources--
		rp.mutex.Unlock()
	}
	rp.mutex.Lock()
	nr = rp.numResources
	rp.mutex.Unlock()
	if rp.maxResources != -1 && nr >= rp.maxResources {
		// we need to wait until something comes back in
		//fmt.Println("acquire is waiting")
		rp.activeWaits = append(rp.activeWaits, acq)
		return
	}

	//fmt.Println("acquire is making one")
	r, err := rp.factory()
	if err != nil {
		acq.ech <- err
	} else {
		rp.mutex.Lock()
		rp.numResources++
		rp.mutex.Unlock()
		acq.rch <- r
	}

	return
}

func (rp *ResourcePool) release(resource Resource) {
	//fmt.Println("release is checking state")
	cr, isCloser := resource.(closer)
	if resource == nil || (isCloser && cr.IsClosed()) {
		//fmt.Println("release is discarding nil or closed resource")
		// don't put it back in the pool.
		rp.mutex.Lock()
		rp.numResources--
		rp.mutex.Unlock()
		return
	}
	if rp.idleCapacity != -1 && rp.idleResources.Size() == rp.idleCapacity {
		//fmt.Println("release is discarding resource because it is excess")
		if isCloser {
			cr.Close()
		}
		rp.mutex.Lock()
		rp.numResources--
		rp.mutex.Unlock()
		return
	}
	//fmt.Println("release is making one available")

	rp.idleResources.Enqueue(resource)
}

func getAcquireMessage() acquireMessage {
	var acq acquireMessage
	var ok bool = false
	m := acqRing.get()
	if m != nil {
		acq, ok = m.(acquireMessage)
	}
	if !ok {
		acq = acquireMessage{
			rch: make(chan Resource),
			ech: make(chan error),
		}
	}
	return acq
}

// Acquire() will get one of the idle resources, or create a new one.
func (rp *ResourcePool) Acquire() (resource Resource, err error) {
	acq := getAcquireMessage()
	rp.acqchan <- acq

	select {
	case resource = <-acq.rch:
	case err = <-acq.ech:
	}

	return
}

func getReleaseMessage() releaseMessage {
	var rel releaseMessage
	var ok bool = false
	m := relRing.get()
	if m != nil {
		rel, ok = m.(releaseMessage)
	}
	if !ok {
		rel = releaseMessage{}
	}
	return rel
}

// Release() will release a resource for use by others. If the idle queue is
// full, the resource will be closed.
func (rp *ResourcePool) Release(resource Resource) {
	rel := getReleaseMessage()
	rel.r = resource
	rp.rchan <- rel
}

func getCloseMessage() closeMessage {
	var cm closeMessage
	var ok bool = false
	m := clsRing.get()
	if m != nil {
		cm, ok = m.(closeMessage)
	}
	if !ok {
		cm = closeMessage{}
	}
	return cm
}

// Close() closes all the pools resources.
func (rp *ResourcePool) Close() {
	cm := getCloseMessage()
	rp.cchan <- cm
}

// NumResources() the number of resources known at this time
func (rp *ResourcePool) NumResources() int {
	rp.mutex.Lock()
	nr := rp.numResources
	rp.mutex.Unlock()
	return nr
}
