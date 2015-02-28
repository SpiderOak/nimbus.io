package main

import (
	"log"
	"os"
	"runtime"
	"sync"
)

// main entry point for webmonitor
func main() {
	var err error
	var file *os.File
	var configSlice []Config

	log.SetFlags(0) // suppress date/time: svlogd supplies that
	log.Printf("info: program starts")

	// load the JSON config file from disk
	if file, err = os.Open(os.Args[1]); err != nil {
		log.Fatalf("unable to open config file %s; %s", os.Args[1], err)
	}
	if configSlice, err = loadConfig(file); err != nil {
		log.Fatalf("critical: error loading config: %s", err)
	}
	file.Close()

	// start a pinger for each config entry
	log.Printf("debug: starting %d pingers", len(configSlice))
	var waitgroup sync.WaitGroup
	for _, config := range configSlice {
		waitgroup.Add(1)
		go func(config Config) {
			defer waitgroup.Done()
			pinger(config)
		}(config)
	}

	log.Printf("debug: waiting on pingers")
	waitgroup.Wait()

	log.Printf("info: program terminates")
}

func setMaxProcs() {
	// if max procs is specified in the environment, leave it
	// otherwise set it to one less than the number of available cores
	maxProcsStr := os.Getenv("GOMAXPROCS")
	if maxProcsStr == "" {
		maxProcs := runtime.NumCPU() - 1
		if maxProcs < 1 {
			maxProcs = 1
		}
		log.Printf("info: setting GOMAXPROCS to %d internally", maxProcs)
		runtime.GOMAXPROCS(maxProcs)
	} else {
		log.Printf("info: GOMAXPROCS set to %s in environment", maxProcsStr)
	}
}
