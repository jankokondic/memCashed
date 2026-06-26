package main

import (
	"log"
	_ "net/http/pprof"

	"github.com/WatchJani/memCashed/memcached/server"
)

func main() {
	// runtime.SetMutexProfileFraction(1)
	// runtime.SetBlockProfileRate(1)

	// go func() {
	// 	log.Println("pprof running on http://localhost:6060")
	// 	log.Println(http.ListenAndServe("localhost:6060", nil))
	// }()

	if err := server.New().Run(); err != nil {
		log.Println(err)
	}
}
