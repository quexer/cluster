package cluster

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/memberlist"
	"github.com/quexer/utee"
	"gopkg.in/quexer/tok.v3"
	"log"
	"net/url"
	"stathat.com/c/consistent"
	"strconv"
	"sync"
	"time"
)

type Config struct {
	//GossipAddr, ip:port, is used for both UDP and TCP gossip
	GossipAddr string
	//Seeds Cluster seed list,
	Seeds []string

	//DataExchangePort port for inter-node data exchange. TCP only.
	DataExchangePort uint32
	//DataExchangeCertPath cert path for inter-node data exchange. self-signed is OK
	DataExchangeCertPath string
	//DataExchangeKeyPath key path for inter-node data exchange.
	DataExchangeKeyPath string
}

type Cluster struct {
	sync.RWMutex
	running    bool
	localName  string
	nodeMap    map[string]*memberlist.Node
	l          *memberlist.Memberlist
	c          *consistent.Consistent
	hub        *tok.Hub
	gOrphanMap utee.SyncMap
}

func CreateCluster(hub *tok.Hub, c *Config) (*Cluster, error) {

	host, port, err := utee.ParseAddr(c.GossipAddr)
	if err != nil {
		return nil, err
	}

	config := memberlist.DefaultLANConfig()
	config.BindAddr = host
	config.BindPort = port
	config.AdvertisePort = port

	config.Delegate = &ClusterDelegate{dataExchangePort: c.DataExchangePort}
	config.Events = &EventDelegate{}

	nodeList, err := memberlist.Create(config)
	if err != nil {
		return nil, err
	}

	if len(c.Seeds) > 0 {
		_, err = nodeList.Join(c.Seeds)
		if err != nil {
			return nil, err
		}
	}
	for _, member := range nodeList.Members() {
		var selfFlag string
		if config.Name == member.Name {
			selfFlag = "self"
		}
		fmt.Printf("cluster node: %s %s:%d %s\n", member.Name, member.Addr, member.Port, selfFlag)
	}

	cluster := &Cluster{
		localName: config.Name,
		l:         nodeList,
		c:         consistent.New(),
	}
	cluster.Run()
	go initGin(cluster, c)
	return cluster, nil
}

func (p *Cluster) Online(uid interface{}) {
	//if it doesn't belong to me, remote notify
	if f, b := p.BelongTo(uid); !b {
		go clsOnline(f, fmt.Sprint(uid), p.localName, true)
		return
	}

	//if it used to be an orphan, update orphan map and do remote kick
	if host, ok := p.gOrphanMap.Get(uid); ok {
		p.gOrphanMap.Remove(uid)
		if f, err := p.http(host.(string)); err != nil {
			go clsKick(f, fmt.Sprint(uid))
		}
	}
}

func (p *Cluster) Offline(uid interface{}) {
	if f, b := p.BelongTo(uid); !b {
		go clsOnline(f, fmt.Sprint(uid), p.localName, false)
	}
}

func (p *Cluster) Send(to interface{}, data []byte, ttl uint32) error {
	if p.hub.CheckOnline(to) {
		return p.hub.Send(to, data, ttl)
	}

	if f, ok := p.BelongTo(to); !ok {
		go func() {
			locateNode, err := clsQuery(f, fmt.Sprint(to))
			if err != nil {
				utee.Log(err, "remote query err")
				return
			}

			if locateNode == "" {
				//not online around the cluster, local cache
				utee.Log(p.hub.Send(to, data, ttl), "[cluster]")
				return
			}

			//direct send
			if f, err := p.http(locateNode); err != nil {
				utee.Log(err, "unkown locateNode "+locateNode)
			} else {
				clsSend(f, fmt.Sprint(to), data, ttl)
			}
		}()
		return nil
	}

	if s, ok := p.gOrphanMap.Get(to); ok {
		if f, err := p.http(s.(string)); err == nil {
			go clsSend(f, fmt.Sprint(to), data, ttl)
			return nil
		} else {
			return err
		}
	} else {
		//it's my user and not on any other node, cache it
		return p.hub.Send(to, data, ttl)
	}

}

//BelongTo return target node invoke function and whether it's local user
func (p *Cluster) BelongTo(key interface{}) (func(string, url.Values) ([]byte, error), bool) {
	p.RLock()
	defer p.RUnlock()

	name, err := p.c.Get(fmt.Sprint(key))
	if err != nil {
		log.Println("[cluster]", err)
		return nil, false
	}

	if f, err := p.http(name); err != nil {
		log.Println(err)
		return nil, false
	} else {
		return f, name == p.localName
	}
}

func (p *Cluster) http(name string) (func(string, url.Values) ([]byte, error), error) {
	p.RLock()
	defer p.RUnlock()

	node := p.nodeMap[name]

	if node == nil {
		return nil, fmt.Errorf("[warn] unkown node %s", name)
	}

	innerHttp2Port, err := strconv.Atoi(string(node.Meta))
	if err != nil {
		return nil, fmt.Errorf("[err] remote meta is not port, %v, %v", node.Addr, node.Meta)
	}

	return clsHttp(node.Addr.String(), innerHttp2Port), nil
}

func (p *Cluster) refresh() {
	p.Lock()
	defer p.Unlock()

	l := []string{}
	m := map[string]*memberlist.Node{}

	for _, node := range p.l.Members() {
		m[node.Name] = node
		l = append(l, node.Name)
	}

	p.nodeMap = m
	p.c.Set(l)
}

func (p *Cluster) Run() {
	p.Lock()
	defer p.Unlock()

	if p.running {
		return
	}
	p.running = true
	go func() {
		for range time.Tick(time.Second * 10) {
			p.refresh()
		}
	}()
}

//cluster base http function
func clsHttp(host string, port int) func(api string, data url.Values) ([]byte, error) {
	return func(api string, data url.Values) ([]byte, error) {
		s := fmt.Sprintf("https://%v:%d/inner/%s", host, port, api)
		if data == nil {
			return utee.HttpGet(s)
		} else {
			return utee.HttpPost(s, data)
		}
	}
}

func clsOnline(f func(string, url.Values) ([]byte, error), id, currentNodeName string, up bool) {
	var api string
	if up {
		api = "online"
	} else {
		api = "offline"
	}

	_, err := f(api, url.Values{"id": {id}, "node": {currentNodeName}})
	utee.Log(err, "clsOnline")
}

func clsKick(f func(string, url.Values) ([]byte, error), id string) {
	_, err := f("kick", url.Values{"id": {id}})
	utee.Log(err, "clsKick")
}

func clsSend(f func(string, url.Values) ([]byte, error), id string, b []byte, ttl uint32) {
	s := base64.StdEncoding.EncodeToString(b)
	t := ""
	if ttl > 0 {
		t = fmt.Sprint(ttl)
	}
	_, err := f("send", url.Values{"id": {id}, "data": {s}, "ttl": {t}})
	utee.Log(err, "clsSend")
}

func clsQuery(f func(string, url.Values) ([]byte, error), id string) (string, error) {
	api := "query?" + url.Values{"id": {id}}.Encode()
	b, err := f(api, nil)
	if err != nil {
		return "", err
	}

	m := map[string]string{}
	err = json.Unmarshal(b, m)
	if err != nil {
		return "", err
	}

	return m["node"], nil
}
