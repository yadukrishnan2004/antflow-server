package workflow

import "fmt"


type State string

const (
	StateCreated   State = "CREATED"
	StateRunning   State = "RUNNING"
	StateCompleted State = "COMPLETED"
	StateFailed    State = "FAILED"
	StateCancelled State = "CANCELLED"
	ActiveStatusActive State = "ACTIVE"
	ActiveStatusDELETE State = "DELETED"
)


var validTransitions = map[State][]State{
	StateCreated:   {StateRunning, StateCancelled},
	StateRunning:   {StateCompleted, StateFailed, StateCancelled},
	StateCompleted: {},
	StateFailed:    {},
	StateCancelled: {},
}

func ValidateTransition(current, next State) error {
	for _, allowed := range validTransitions[current] {
		if allowed == next {
			return nil
		}
	}
	return fmt.Errorf("invalid state transition: %s → %s", current, next)
}

// IsTerminal returns true if no further transitions are possible from this state.
func IsTerminal(s State) bool {
	return len(validTransitions[s]) == 0
}


