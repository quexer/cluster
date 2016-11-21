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

//Config used to create new cluster
type Config struct {
	//GossipAddr, ip:port, is used for both UDP and TCP gossip
	GossipAddr string
	//Seeds Cluster seed list,
	Seeds []string

	//DataExchangePort port for inter-node data exchange. TCP only.
	DataExchangePort uint
	//DataExchangeCertPath cert path for inter-node data exchange. self-signed is OK
	DataExchangeCertPath string
	//DataExchangeKeyPath key path for inter-node data exchange.
	DataExchangeKeyPath string
}

//Cluster
type Cluster struct {
	sync.RWMutex
	localName string
	nodeMap   map[string]*memberlist.Node
	l         *memberlist.Memberlist
	c         *consistent.Consistent
	hub       *tok.Hub
	orphanMap utee.SyncMap
}

func CreateCluster(hub *tok.Hub, c *Config) (*Cluster, error) {

	host, port, err := utee.ParseAddr(c.GossipAddr)
	if err != nil {
		return nil, err
	}

	if host == "0.0.0.0" || host == "127.0.0.1" {
		log.Printf("[warn] cluster can't work on %s, please bind to spesfic ip\n", c.GossipAddr)
	}

	config := memberlist.DefaultLANConfig()
	config.BindAddr = host
	config.BindPort = port
	config.AdvertisePort = port

	config.Delegate = &delegate{dataExchangePort: c.DataExchangePort}
	config.Events = &eventDelegate{}

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
		hub:       hub,
	}

	go initGin(cluster, c)
	go func() {
		for range time.Tick(time.Second * 10) {
			cluster.refresh()
		}
	}()
	return cluster, nil
}

func (p *Cluster) Online(uid interface{}) {
	//if it doesn't belong to me, remote notify
	if f, b := p.belongTo(uid); !b {
		go clsOnline(f, fmt.Sprint(uid), p.localName, true)
		return
	}

	//if it used to be an orphan, update orphan map and do remote kick
	if host, ok := p.orphanMap.Get(uid); ok {
		p.orphanMap.Remove(uid)
		if f, err := p.http(host.(string)); err != nil {
			go clsKick(f, fmt.Sprint(uid))
		}
	}
}

func (p *Cluster) Offline(uid interface{}) {
	if f, b := p.belongTo(uid); !b {
		go clsOnline(f, fmt.Sprint(uid), p.localName, false)
	}
}

func (p *Cluster) Send(to interface{}, data []byte, ttl uint32) error {
	if p.hub.CheckOnline(to) {
		return p.hub.Send(to, data, ttl)
	}

	if f, ok := p.belongTo(to); !ok {
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

	if s, ok := p.orphanMap.Get(to); ok {
		f, err := p.http(s.(string))
		if err == nil {
			go clsSend(f, fmt.Sprint(to), data, ttl)
		}
		return err
	}

	//it's my user and not on any other node, cache it
	return p.hub.Send(to, data, ttl)
}

//onNodeChange
//loop all online user, check affiliation, notify if necessary. break if stale
//update orphan list, check affiliation, remove from list if necessary.  break if stale
func (p *Cluster) onNodeChange(fStale func(time.Time) bool, t time.Time) {
	log.Println("[cluster] node change, check online users")
	//check online user, if it doesn't belong to me,
	for _, id := range p.hub.Online() {
		if fStale(t) {
			break
		}
		if f, local := p.belongTo(id); !local {
			//todo async ??
			clsOnline(f, p.localName, fmt.Sprint(id), true)
		}
	}

	log.Println("[cluster] node change, update orphan list")
	//update orphan list
	for _, k := range p.orphanMap.Keys() {
		if _, local := p.belongTo(k); !local {
			p.orphanMap.Remove(k)
		}
	}
}

//BelongTo return target node invoke function and whether it's local user
func (p *Cluster) belongTo(key interface{}) (func(string, url.Values) ([]byte, error), bool) {
	p.RLock()
	defer p.RUnlock()

	name, err := p.c.Get(fmt.Sprint(key))
	if err != nil {
		log.Println("[cluster]", err)
		return nil, false
	}

	f, err := p.http(name)
	if err != nil {
		log.Println(err)
		return nil, false
	}

	return f, name == p.localName
}

func (p *Cluster) http(name string) (func(string, url.Values) ([]byte, error), error) {
	p.RLock()
	defer p.RUnlock()

	node := p.nodeMap[name]

	if node == nil {
		return nil, fmt.Errorf("[warn] unkown node %s", name)
	}

	port, err := strconv.Atoi(string(node.Meta))
	if err != nil {
		return nil, fmt.Errorf("[err] remote meta is not port, %v, %v", node.Addr, node.Meta)
	}

	return clsHTTP(node.Addr.String(), port), nil
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

//cluster base http function
func clsHTTP(host string, port int) func(api string, data url.Values) ([]byte, error) {
	return func(api string, data url.Values) ([]byte, error) {
		s := fmt.Sprintf("https://%v:%d/inner/%s", host, port, api)
		if data == nil {
			return utee.HttpGet(s)
		}

		return utee.HttpPost(s, data)
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
