package redisRegistry

var _ NodeEventIface = (*NodeManager)(nil)

type NodeEventIface interface {
	RegisterCallback(name string, callback func(NodeEvent)) error
}

type NodeEvent struct {
	NodeID       string         // Current node ID
	NodeCount    int            // Latest total number of nodes
	OldCount     int            // Total number before change
	SelfPosition int            // Latest position of the current node
	OldPos       int            // Position before change
	ChangeType   string         // Type of change: join/rejoin/failed/leave/update/init
	Timestamp    int64          // Event timestamp (in milliseconds)
	NodeList     []NodeInfoResp // Latest list of active nodes
}
