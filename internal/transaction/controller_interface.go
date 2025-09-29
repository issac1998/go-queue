package transaction

// ControllerInterface defines the interface for TransactionManager to interact with the controller
type ControllerInterface interface {
	// RegisterProducerGroup registers a producer group with the controller
	// callbackAddr is the address where the controller can send transaction check requests
	RegisterProducerGroup(groupID string, callbackAddr string) error
	
	// UnregisterProducerGroup removes a producer group from the controller
	UnregisterProducerGroup(groupID string) error
	
	// GetProducerGroups retrieves all registered producer groups from the controller
	// Returns map[groupID]callbackAddr for transaction check purposes
	GetProducerGroups() (map[string]string, error)
	
	// IsControllerLeader checks if the current node is the controller leader
	IsControllerLeader() bool
}