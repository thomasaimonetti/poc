package messaging

const (
	// User management channels
	SubjectUsersUpdate    = "users.push.*" // Frontend → Backend (user operations)
	SubjectUsersBroadcast = "users.pull.*" // Backend → Frontend (real-time updates)

	// Health and system events
	SubjectHealth  = "system.health"
	SubjectMetrics = "system.metrics"
)
