package client

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/issac1998/go-queue/internal/errors"
	"github.com/issac1998/go-queue/internal/protocol"
)

// connectForMetadata creates a connection for metadata operations with read preference
// isWrite: true for write operations (must go to Controller Leader)
func (c *Client) connectForMetadata(isWrite bool) (net.Conn, error) {
	if isWrite {
		return c.connectToController()
	} else {
		return c.connectToFollower()
	}
}

// connectToController connects to the actual controller leader with verification
func (c *Client) connectToController() (net.Conn, error) {
	controllerAddr := c.GetControllerAddr()
	if controllerAddr != "" {
		if conn, err := c.connectAndVerifyController(controllerAddr); err == nil {
			return conn, nil
		}
		// Cached address is invalid, clear it
		c.setControllerAddr("")
	}

	if err := c.DiscoverController(); err != nil {
		return nil, &errors.TypedError{
			Type:    errors.ControllerError,
			Message: "failed to discover controller",
			Cause:   err,
		}
	}

	controllerAddr = c.GetControllerAddr()
	if controllerAddr == "" {
		return nil, &errors.TypedError{
			Type:    errors.ControllerError,
			Message: errors.ControllerNotAvailableMsg,
		}
	}

	return c.connectAndVerifyController(controllerAddr)
}

// connectAndVerifyController connects to a broker and verifies it's the controller leader
// TODO:DO we have a better way to verify controller leader?like just send a request to the broker,
// proxy to controller leader and then return an error to indicate local cache is stale?
func (c *Client) connectAndVerifyController(brokerAddr string) (net.Conn, error) {
	// First verify with a separate connection
	verifyConn, err := net.DialTimeout("tcp", brokerAddr, c.timeout)
	if err != nil {
		return nil, &errors.TypedError{
			Type:    errors.ConnectionError,
			Message: fmt.Sprintf("failed to connect to %s for verification", brokerAddr),
			Cause:   err,
		}
	}

	if err := c.verifyControllerLeader(verifyConn); err != nil {
		verifyConn.Close()
		return nil, &errors.TypedError{
			Type:    errors.ControllerError,
			Message: fmt.Sprintf("broker %s is not controller leader", brokerAddr),
			Cause:   err,
		}
	}
	verifyConn.Close()

	// Now create a fresh connection for actual requests
	conn, err := net.DialTimeout("tcp", brokerAddr, c.timeout)
	if err != nil {
		return nil, &errors.TypedError{
			Type:    errors.ConnectionError,
			Message: fmt.Sprintf("failed to connect to %s", brokerAddr),
			Cause:   err,
		}
	}

	return conn, nil
}

// verifyControllerLeader sends a verification request to check if the broker is controller leader
func (c *Client) verifyControllerLeader(conn net.Conn) error {
	conn.SetDeadline(time.Now().Add(c.timeout))

	requestType := protocol.ControllerVerifyRequestType
	if err := binary.Write(conn, binary.BigEndian, requestType); err != nil {
		return fmt.Errorf("failed to send verification request: %v", err)
	}

	// Send empty data length for protocol consistency
	dataLength := int32(0)
	if err := binary.Write(conn, binary.BigEndian, dataLength); err != nil {
		return fmt.Errorf("failed to send data length: %v", err)
	}

	var responseLen int32
	if err := binary.Read(conn, binary.BigEndian, &responseLen); err != nil {
		return fmt.Errorf("failed to read verification response length: %v", err)
	}

	responseData := make([]byte, responseLen)
	if _, err := io.ReadFull(conn, responseData); err != nil {
		return fmt.Errorf("failed to read verification response: %v", err)
	}

	isController := string(responseData) == "true"
	if !isController {
		return &errors.TypedError{
			Type:    errors.ControllerError,
			Message: errors.ControllerNotAvailableMsg,
		}
	}

	return nil
}

// connectToAnyBroker connects to Follower
func (c *Client) connectToFollower() (net.Conn, error) {
	candidates := make([]string, 0, len(c.brokerAddrs))
	controllerAddr := c.GetControllerAddr()

	for _, replica := range c.brokerAddrs {
		if replica != controllerAddr { // Exclude leader from replicas list
			candidates = append(candidates, replica)
		}
	}

	var selectedBroker string
	if len(candidates) > 0 {
		selectedBroker = c.selectFollower(candidates)
		if selectedBroker == "" && controllerAddr != "" {
			selectedBroker = controllerAddr
		}
	} else if controllerAddr != "" {
		selectedBroker = controllerAddr
	}

	if selectedBroker == "" {
		return nil, fmt.Errorf("failed to connect to any broker")
	}

	return protocol.ConnectToSpecificBroker(selectedBroker, c.timeout)
}

// connectForDataOperation connects to the appropriate broker for data operations
// Routes to partition leader for writes, or follower for reads
func (c *Client) connectForDataOperation(topic string, partition int32, isWrite bool) (net.Conn, error) {
	metadata, err := c.getTopicMetadata(topic)
	if err != nil {
		return nil, fmt.Errorf("failed to get topic metadata: %v", err)
	}

	partitionMeta, exists := metadata.Partitions[partition]
	if !exists {
		return nil, fmt.Errorf("partition %d not found for topic %s", partition, topic)
	}

	var targetBroker string
	if isWrite {
		targetBroker = partitionMeta.Leader
	} else {
		targetBroker = c.selectBrokerForRead(partitionMeta)
	}

	return protocol.ConnectToSpecificBroker(targetBroker, c.timeout)
}

// selectBrokerForRead implements intelligent broker selection for read operations
// TODO:Client cache for follwer(includes delete follower when faielde)
func (c *Client) selectBrokerForRead(partitionMeta PartitionMetadata) string {
	candidates := make([]string, 0, len(partitionMeta.Replicas)+1)

	// Exclude leader from replicas list
	for _, replica := range partitionMeta.Replicas {
		if replica != partitionMeta.Leader {
			candidates = append(candidates, replica)
		}
	}

	if len(candidates) == 0 {
		return partitionMeta.Leader
	}

	if len(candidates) > 1 {
		selectedBroker := c.selectFollower(candidates)
		if selectedBroker != "" {
			log.Printf("Selected follower for read: %s", selectedBroker)
			return selectedBroker
		}
	}

	log.Printf("Fallback to leader for read: %s", partitionMeta.Leader)
	return partitionMeta.Leader
}

// selectFollower selects a follower
// TODO :use round robin to select broker
func (c *Client) selectFollower(followers []string) string {
	if len(followers) == 0 {
		return ""
	}

	selectedIndex := int(time.Now().UnixNano()) % len(followers)
	selectedFollower := followers[selectedIndex]

	if c.isFollowerAvailable(selectedFollower) {
		return selectedFollower
	}

	for i, follower := range followers {
		if i != selectedIndex && c.isFollowerAvailable(follower) {
			return follower
		}
	}

	return ""
}

// isFollowerAvailable performs a quick availability check for a follower
// TODO:DO we really need to check if we can connect to the broker? can we delete it ?same as connectToController?
func (c *Client) isFollowerAvailable(brokerAddr string) bool {
	conn, err := net.DialTimeout("tcp", brokerAddr, 100*time.Millisecond)
	if err != nil {
		log.Printf("Follower %s not available: %v", brokerAddr, err)
		return false
	}
	conn.Close()
	return true
}
