package main

import (
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
)

// main entry point for webmonitor
func main() {
	var err error
	var file *os.File
	var configSlice []Config
	var hostAvailChan chan<- HostAvailForAddress

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

	// set up a signal handling channel
	signalChannel := make(chan os.Signal)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)

	// channel to signal end of job
	haltChan := make(chan struct{})

	// set up the redis sink
	if hostAvailChan, err = NewRedisHostAvailSink(); err != nil {
		log.Fatalf("critical: unable to create redis sink %s", err)
	}

	// start a pinger for each config entry
	log.Printf("debug: starting %d pingers", len(configSlice))
	var waitgroup sync.WaitGroup
	for _, config := range configSlice {
		waitgroup.Add(1)
		go func(config Config) {
			defer waitgroup.Done()
			pinger(haltChan, hostAvailChan, config)
		}(config)
	}

	// wait for a signal, like SIGTERM
	log.Printf("debug: main thread waiting for SIGTERM")
	signal := <-signalChannel
	log.Printf("info: terminated by signal %v", signal)

	// notify the pingers by closing the channel
	close(haltChan)

	log.Printf("debug: waiting for pingers to halt")
	waitgroup.Wait()

	close(hostAvailChan)

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
