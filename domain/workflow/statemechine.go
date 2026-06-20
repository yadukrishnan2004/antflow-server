package workflow

import "fmt"

type State string

const (
	StateCreated      State = "CREATED"
	StateRunning      State = "RUNNING"
	StateCompensating State = "COMPENSATING"
	StateCompleted    State = "COMPLETED"
	StateFailed       State = "FAILED"
	StateCancelled    State = "CANCELLED"
)

var validTransitions = map[State][]State{
	StateCreated:      {StateRunning, StateCancelled},
	StateRunning:      {StateCompleted, StateFailed, StateCancelled, StateCompensating},
	StateCompensating: {StateFailed, StateCancelled},
	StateCompleted:    {},
	StateFailed:       {},
	StateCancelled:    {},
}

func ValidateTransition(current, next State) error {
	for _, allowed := range validTransitions[current] {
		if allowed == next {
			return nil
		}
	}
	return fmt.Errorf("%w: %s → %s", ErrInvalidStateTransition, current, next)
}

// IsTerminal returns true if no further transitions are possible from this state.
func IsTerminal(s State) bool {
	return len(validTransitions[s]) == 0
}
