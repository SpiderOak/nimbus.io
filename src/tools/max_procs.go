package tools

import (
	"log"
	"os"
	"runtime"
)

func SetMaxProcs() {
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
