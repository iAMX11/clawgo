package queue

import (
	"context"
	"testing"
)

func TestEnqueueReturnsFalseWhenFull(t *testing.T) {
	q := New(1)
	if ok := q.Enqueue(func(context.Context) error { return nil }); !ok {
		t.Fatalf("first enqueue should succeed")
	}
	if ok := q.Enqueue(func(context.Context) error { return nil }); ok {
		t.Fatalf("second enqueue should fail when queue is full")
	}
}

func TestEnqueueReturnsFalseAfterClose(t *testing.T) {
	q := New(1)
	q.Close()
	if ok := q.Enqueue(func(context.Context) error { return nil }); ok {
		t.Fatalf("enqueue should fail after close")
	}
}
