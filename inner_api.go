package cluster

import (
	"encoding/base64"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/quexer/utee"
	"log"
	"net/http"
	"strconv"
	"strings"
)

const ginKeyCluster = "ginKeyCluster"

func initGin(cluster *Cluster, conf *Config) {
	log.Println("[cluster]init inner http2 server on", conf.DataExchangePort)
	router := gin.Default()

	g := router.Group("/inner")
	g.Use(func(c *gin.Context) {
		c.Set(ginKeyCluster, cluster)
	})
	g.POST("/online", ginOnline)
	g.POST("/offline", ginOffline)
	g.POST("/kick", ginKick)
	g.POST("/send", ginSend)
	g.GET("/query", ginQuery)

	http.ListenAndServeTLS(fmt.Sprintf(":%d", conf.DataExchangePort), conf.DataExchangeCertPath, conf.DataExchangeKeyPath, router)
}

//one of my user has got online on other node
func ginOnline(c *gin.Context) {
	id := c.PostForm("id")
	node := c.PostForm("node")

	if empty(id, node) {
		c.JSON(400, gin.H{"status": "id, node are required"})
		return
	}

	uid, err := strconv.Atoi(id)
	if err != nil {
		c.JSON(400, gin.H{"status": "bad id:" + id})
	}

	cluster := c.MustGet(ginKeyCluster).(*Cluster)

	if _, b := cluster.belongTo(uid); !b {
		log.Printf("[warn] weird,  %s not belong to me\n", id)
	}

	cluster.hub.Kick(uid)

	if s, ok := cluster.orphanMap.Get(uid); ok {
		if oldNode := s.(string); oldNode != node {
			if f, err := cluster.http(oldNode); err != nil {
				utee.Log(err, "[remote online] unkown node "+oldNode)
			} else {
				go clsKick(f, fmt.Sprint(uid))
			}
		}
	}
	cluster.orphanMap.Put(uid, node)

	c.JSON(200, gin.H{"status": "ok"})
}

func ginOffline(c *gin.Context) {
	id := c.PostForm("id")
	node := c.PostForm("node")
	if empty(id, node) {
		c.JSON(400, gin.H{"status": "id, node are required"})
		return
	}

	uid, err := strconv.Atoi(id)
	if err != nil {
		c.JSON(400, gin.H{"status": "bad id:" + id})
	}

	cluster := c.MustGet(ginKeyCluster).(*Cluster)

	if _, b := cluster.belongTo(uid); !b {
		log.Printf("[warn] weird,  %s not belong to me\n", id)
	}

	s, ok := cluster.orphanMap.Get(uid)
	if ok {
		if s.(string) == node {
			cluster.orphanMap.Remove(uid)
		} else {
			//maybe a new remote connection has kicked the old one
			log.Printf("[warn] node inconsist, assume %v on %v but on %s\n", uid, s, node)
		}
	}
	c.JSON(200, gin.H{"status": "ok"})
}

//remote send
func ginSend(c *gin.Context) {
	id := c.PostForm("id")
	data := c.PostForm("data")
	ttl := c.PostForm("ttl")
	if empty(id, data, ttl) {
		c.JSON(400, gin.H{"status": "id, data are required"})
		return
	}

	uid, err := strconv.Atoi(id)
	if err != nil {
		c.JSON(400, gin.H{"status": "bad id:" + id})
		return
	}

	b, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		c.JSON(400, gin.H{"status": "bad data, decode error, " + data})
		return
	}

	cluster := c.MustGet(ginKeyCluster).(*Cluster)

	n, err := strconv.Atoi(ttl)
	if err != nil || n < 0 {
		c.JSON(400, gin.H{"status": "bad ttl:" + ttl})
		return
	}

	if err = cluster.hub.Send(uid, b, uint32(n)); err != nil {
		utee.Log(err, "send")
		c.JSON(500, gin.H{"status": "send err"})
		return
	}

	c.JSON(200, gin.H{"status": "ok"})
}

// remote kick
func ginKick(c *gin.Context) {
	id := c.PostForm("id")

	uid, err := strconv.Atoi(id)
	if err != nil {
		c.JSON(400, gin.H{"status": "bad id:" + id})
	}

	cluster := c.MustGet(ginKeyCluster).(*Cluster)

	if _, b := cluster.belongTo(uid); !b {
		log.Printf("[warn] weird, remote kicking my user %s ?\n", id)
	}

	cluster.hub.Kick(uid)
	c.JSON(200, gin.H{"status": "ok"})
}

//return node name if user online, else ""
func ginQuery(c *gin.Context) {
	id := c.Query("id")
	uid, err := strconv.Atoi(id)
	if err != nil {
		c.JSON(400, gin.H{"status": "bad id:" + id})
	}

	nodeName := ""

	cluster := c.MustGet(ginKeyCluster).(*Cluster)

	if cluster.hub.CheckOnline(uid) {
		nodeName = cluster.localName
	} else {
		if s, ok := cluster.orphanMap.Get(uid); ok {
			nodeName = s.(string)
		}
	}
	c.JSON(200, gin.H{"status": "ok", "node": nodeName})
}

func empty(l ...string) bool {
	for _, s := range l {
		if strings.TrimSpace(s) != "" {
			return false
		}
	}
	return true
}
