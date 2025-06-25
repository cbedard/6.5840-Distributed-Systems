package raft

import (
	"log"
	"time"
)

// Debugging
const Debug = true

var startTime = time.Now().UnixMilli()

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func timeSinceStart() int64 {
	return time.Now().UnixMilli() - startTime
}
