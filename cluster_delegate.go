package cluster

import (
	"fmt"
	"github.com/hashicorp/memberlist"
	"log"
	"sync"
	"time"
)

type ClusterDelegate struct {
	dataExchangePort uint32
}

func (p *ClusterDelegate) NodeMeta(limit int) []byte {
	return []byte(fmt.Sprint(p.dataExchangePort))
}

func (p *ClusterDelegate) NotifyMsg(b []byte) {
}

func (p *ClusterDelegate) GetBroadcasts(overhead, limit int) [][]byte {
	return nil
}

func (p *ClusterDelegate) LocalState(join bool) []byte {
	return nil
}

func (p *ClusterDelegate) MergeRemoteState(buf []byte, join bool) {
}

type EventDelegate struct {
	sync.RWMutex
	cluster *Cluster
	mt      time.Time
}

func (p *EventDelegate) NotifyJoin(*memberlist.Node) {
	log.Println("someone join... recheck")
	p.run(time.Now())
}

func (p *EventDelegate) NotifyLeave(*memberlist.Node) {
	log.Println("someone leave... recheck")
	p.run(time.Now())
}

func (p *EventDelegate) NotifyUpdate(*memberlist.Node) {
}

func (p *EventDelegate) stale(t time.Time) bool {
	p.RLock()
	defer p.RUnlock()
	return p.mt.After(t)
}

func (p *EventDelegate) run(occurrence time.Time) {
	p.Lock()
	defer p.Unlock()
	p.mt = occurrence

	//calm down for 30s
	go func(t time.Time) {
		time.Sleep(time.Second * 30)
		p.reCheck(t)
	}(occurrence)
}

//reCheck
//loop all online user, check affiliation, notify if necessary. break if stale
//update orphan list, break if stale
func (p *EventDelegate) reCheck(t time.Time) {
	//check online user
	for _, id := range p.cluster.hub.Online() {
		if p.stale(t) {
			break
		}
		if f, remote := p.cluster.BelongTo(id); remote {
			//todo async ??
			clsOnline(f, p.cluster.localName, fmt.Sprint(id), true)
		}
	}

	//todo update orphan list
}
