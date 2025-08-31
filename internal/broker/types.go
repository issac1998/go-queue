package broker

import (
	"github.com/issac1998/go-queue/internal/discovery"
	"github.com/issac1998/go-queue/internal/raft"
)

// Type aliases for configuration
type RaftConfig = raft.RaftConfig
type DiscoveryConfig = discovery.DiscoveryConfig
