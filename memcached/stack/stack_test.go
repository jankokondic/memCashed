package stack

import (
	"errors"
	"testing"
)

func TestNewStackIsEmpty(t *testing.T) {
	s := New[int](10)

	if !s.IsEmpty() {
		t.Fatal("expected new stack to be empty")
	}
}

func TestPushAndPop(t *testing.T) {
	s := New[int](10)

	s.Push(1)
	s.Push(2)
	s.Push(3)

	value, err := s.Pop()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if value != 3 {
		t.Fatalf("expected 3, got %d", value)
	}

	value, err = s.Pop()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if value != 2 {
		t.Fatalf("expected 2, got %d", value)
	}

	value, err = s.Pop()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if value != 1 {
		t.Fatalf("expected 1, got %d", value)
	}

	if !s.IsEmpty() {
		t.Fatal("expected stack to be empty")
	}
}

func TestPopEmptyReturnsError(t *testing.T) {
	s := New[int](10)

	value, err := s.Pop()

	if !errors.Is(err, ErrEmpty) {
		t.Fatalf("expected ErrEmpty, got %v", err)
	}

	if value != 0 {
		t.Fatalf("expected zero value 0, got %d", value)
	}
}

func TestPeekDoesNotRemoveValue(t *testing.T) {
	s := New[string](10)

	s.Push("first")
	s.Push("second")

	value, err := s.Peek()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if value != "second" {
		t.Fatalf("expected %q, got %q", "second", value)
	}

	value, err = s.Pop()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if value != "second" {
		t.Fatalf("expected %q after peek, got %q", "second", value)
	}
}

func TestPeekEmptyReturnsError(t *testing.T) {
	s := New[string](10)

	value, err := s.Peek()

	if !errors.Is(err, ErrEmpty) {
		t.Fatalf("expected ErrEmpty, got %v", err)
	}

	if value != "" {
		t.Fatalf("expected empty string zero value, got %q", value)
	}
}

func TestClear(t *testing.T) {
	s := New[int](10)

	s.Push(1)
	s.Push(2)
	s.Push(3)

	s.Clear()

	if !s.IsEmpty() {
		t.Fatal("expected stack to be empty after Clear")
	}

	_, err := s.Pop()
	if !errors.Is(err, ErrEmpty) {
		t.Fatalf("expected ErrEmpty after Clear, got %v", err)
	}
}

func TestStackWithStruct(t *testing.T) {
	type item struct {
		key   string
		value int
	}

	s := New[item](2)

	s.Push(item{key: "a", value: 1})
	s.Push(item{key: "b", value: 2})

	value, err := s.Pop()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if value.key != "b" || value.value != 2 {
		t.Fatalf("expected {b 2}, got {%s %d}", value.key, value.value)
	}
}
