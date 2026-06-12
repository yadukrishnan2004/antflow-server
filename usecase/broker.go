package usecase

import (
	"sync"
)

type TaskBroker struct {
	mu     sync.RWMutex
	queues map[string][]chan struct{}
}

func NewTaskBroker() *TaskBroker {
	return &TaskBroker{
		queues: make(map[string][]chan struct{}),
	}
}

func (b *TaskBroker) Notify(taskQueue string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	channels, exists := b.queues[taskQueue]
	if !exists {
		return
	}

	// Notify all workers listening on this queue
	for _, ch := range channels {
		select {
		case ch <- struct{}{}:
		default:
			// Non-blocking if channel is already notified
		}
	}
}

func (b *TaskBroker) Subscribe(taskQueue string) (chan struct{}, func()) {
	b.mu.Lock()
	defer b.mu.Unlock()

	ch := make(chan struct{}, 1)
	b.queues[taskQueue] = append(b.queues[taskQueue], ch)

	unsubscribe := func() {
		b.mu.Lock()
		defer b.mu.Unlock()

		channels := b.queues[taskQueue]
		for i, c := range channels {
			if c == ch {
				// Remove channel
				b.queues[taskQueue] = append(channels[:i], channels[i+1:]...)
				close(ch)
				break
			}
		}
	}

	return ch, unsubscribe
}
