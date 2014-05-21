package tools

import (
	"fmt"

	"github.com/pebbe/zmq4"
)

type ZMQVersion struct {
	Major int
	Minor int
	Patch int
}

func GetZMQVersion() ZMQVersion {
	var version ZMQVersion
	version.Major, version.Minor, version.Patch = zmq4.Version()
	return version
}

func (version ZMQVersion) String() string {
	return fmt.Sprintf("ZMQ %d.%d.%d", version.Major, version.Minor, version.Patch)
}
