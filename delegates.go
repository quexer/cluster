package cluster

import (
	"fmt"
	"github.com/hashicorp/memberlist"
	"log"
	"sync"
	"time"
)

type delegate struct {
	dataExchangePort uint
}

func (p *delegate) NodeMeta(limit int) []byte {
	return []byte(fmt.Sprint(p.dataExchangePort))
}

func (p *delegate) NotifyMsg(b []byte) {
}

func (p *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return nil
}

func (p *delegate) LocalState(join bool) []byte {
	return nil
}

func (p *delegate) MergeRemoteState(buf []byte, join bool) {
}

type eventDelegate struct {
	sync.RWMutex
	cluster *Cluster
	mt      time.Time
}

func (p *eventDelegate) NotifyJoin(*memberlist.Node) {
	log.Println("someone join... recheck")
	p.run(time.Now())
}

func (p *eventDelegate) NotifyLeave(*memberlist.Node) {
	log.Println("someone leave... recheck")
	p.run(time.Now())
}

func (p *eventDelegate) NotifyUpdate(*memberlist.Node) {
}

func (p *eventDelegate) stale(t time.Time) bool {
	p.RLock()
	defer p.RUnlock()
	return p.mt.After(t)
}

func (p *eventDelegate) run(now time.Time) {
	p.Lock()
	defer p.Unlock()
	p.mt = now

	//calm down for 30s
	go func(t time.Time) {
		time.Sleep(time.Second * 30)
		p.cluster.onNodeChange(p.stale, t)
	}(now)
}
