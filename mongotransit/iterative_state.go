package mongotransit

import "time"

// IterativeRestoreState represents state or progress of iterative dump for a specific collection
type IterativeRestoreState struct {
	CollectionOption
	Checkpoint time.Time
	Lag        int64
}
