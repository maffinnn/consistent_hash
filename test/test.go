package main

import (
	"log"
	"fmt"
	"net/http"
	"flag"
	"strconv"

	"distributed-file-system/lib/golang/cache"
	ch "distributed-file-system/lib/golang/cache/consistenthash"
)

func testHashing() {
	hash := ch.New(3, func(key []byte) uint32 {
		i, _ := strconv.Atoi(string(key))
		return uint32(i)
	})

	// Given the above hash function, this will give replicas with "hashes":
	// 2, 12, 22
	// 4, 14, 24
	// 6, 16, 26
	hash.Add("2", "4", "6")

	testCases := map[string]string{
		"2":  "2",
		"11": "2",
		"23": "4",
		"13": "4",
		"6":  "6",
		"25": "6",
		"27": "2",
		"28": "2",
	}
	// 2, 4, 6, 12, 14, 16, 22, 24, 26
	for k, v := range testCases {
		if hash.Get(k) != v {
			log.Printf("Asking for %s, should have yielded %s", k, v)
		}
	}
	// Adds 8, 18, 28
	hash.Add("8")
	// 27 should now map to 8.
	testCases["27"] = "8"
	testCases["28"] = "8"
	for k, v := range testCases {
		if hash.Get(k) != v {
			log.Printf("Asking for %s, should have yielded %s", k, v)
		}
	}

}

var db = map[string]string{
	"Tom":  "630",
	"Jack": "589",
	"Sam":  "567",
}


func testCache() {
	loadCounts := make(map[string]int, len(db))
	c := cache.NewGroup("scores", 1<<10, cache.GetterFunc(
		func(key string) ([]byte, error) {
			log.Println("search key", key)
			if v, ok := db[key]; ok {
				if _, ok := loadCounts[key]; !ok {
					loadCounts[key] = 0
				}
				loadCounts[key] += 1
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s not exist", key)
		}))

	for k, v := range db {
		if view, err := c.Get(k); err != nil || view.String() != v {
			log.Fatal("failed to get value of Tom")
		} // load from callback function
		if _, err := c.Get(k); err != nil || loadCounts[k] > 1 {
			log.Fatalf("cache %s miss", k)
		} // cache hit
	}

	if view, err := c.Get("unknown"); err == nil {
		log.Fatalf("the value of unknow should be empty, but %s got", view)
	}
}

func createGroup() *cache.Group {
	return cache.NewGroup("scores", 2<<10, cache.GetterFunc(
		func(key string) ([]byte, error) {
			log.Println("[SlowDB] search key", key)
			if v, ok := db[key]; ok {
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s not exist", key)
		}))
}

func startCacheServer(addr string, addrs []string, c *cache.Group) {
	peers := cache.NewHTTPPool(addr)
	peers.Set(addrs...)
	c.RegisterPeers(peers)
	log.Println("Cache server is running at", addr)
	log.Fatal(http.ListenAndServe(addr[7:], peers))
}

func startAPIServer(apiAddr string, c *cache.Group) {
	http.Handle("/api", http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			key := r.URL.Query().Get("key")
			view, err := c.Get(key)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Write(view.ByteSlice())

		}))
	log.Println("fontend server is running at", apiAddr)
	log.Fatal(http.ListenAndServe(apiAddr[7:], nil))

}

func main() {
	var port int
	var api bool
	flag.IntVar(&port, "port", 8001, "Cache server port")
	flag.BoolVar(&api, "api", false, "Start a api server?")
	flag.Parse()

	apiAddr := "http://localhost:9999"
	addrMap := map[int]string{
		8001: "http://localhost:8001",
		8002: "http://localhost:8002",
		8003: "http://localhost:8003",
	}

	var addrs []string
	for _, v := range addrMap {
		addrs = append(addrs, v)
	}

	cache := createGroup()
	if api {
		go startAPIServer(apiAddr, cache)
	}
	startCacheServer(addrMap[port], []string(addrs), cache)
}
