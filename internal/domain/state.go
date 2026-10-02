// Package domain defines transport-independent acquisition identities and states.
package domain

// State describes durable progress of one execution; recovery creates a new run.
type State string

const (
	Queued             State = "queued"
	Pulling            State = "pulling"
	DownloadsCompleted State = "downloads_completed"
	DeliveryPending    State = "delivery_pending"
	Delivering         State = "delivering"
	Completed          State = "completed"
	SkippedUnchanged   State = "skipped_unchanged"
	Failed             State = "failed"
)

// Terminal reports whether execution history is final and must not be resumed.
func (s State) Terminal() bool {
	return s == Completed || s == SkippedUnchanged || s == Failed
}
