// Package state persists immutable records and conditionally updated coordination.
package state

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"
	"unicode"
)

var (
	ErrNotFound  = errors.New("state object not found")
	ErrConflict  = errors.New("state write precondition failed")
	ErrIntegrity = errors.New("immutable state differs from existing bytes")
	ErrExpired   = errors.New("state record expired")
)

// Object owns its data; ETag is an opaque concurrency token, never a content hash.
type Object struct {
	Data     []byte
	ETag     string
	Modified time.Time
}

// Page preserves the provider continuation token for bounded recovery scans.
type Page struct {
	Keys      []string
	NextToken string
}

// Objects is the conditional object boundary implemented by S3 and test fakes.
type Objects interface {
	Get(context.Context, string) (Object, error)
	Put(context.Context, string, []byte, string) (string, error) // Empty match creates only; otherwise matches ETag.
	List(context.Context, string, string, int32) (Page, error)
}

// Store applies immutable-write verification and coordination-only update rules.
type Store struct{ objects Objects }

// New creates a state repository over a conditional object backend.
func New(objects Objects) *Store { return &Store{objects: objects} }

// Create preserves immutable bytes; a duplicate succeeds only after exact verification.
func (s *Store) Create(ctx context.Context, key string, data []byte) error {
	if !validKey(key) || strings.HasPrefix(key, "coordination/") {
		return fmt.Errorf("invalid immutable key %q", key)
	}
	_, err := s.objects.Put(ctx, key, data, "")
	if err == nil {
		return nil
	}
	// Verify ambiguous failures too: S3 may persist a write before its reply is lost.
	existing, readErr := s.objects.Get(ctx, key)
	if readErr != nil {
		return fmt.Errorf("create %s: %w", key, errors.Join(err, readErr))
	}
	if !bytes.Equal(existing.Data, data) {
		return fmt.Errorf("%s: %w", key, ErrIntegrity)
	}
	return nil
}

// Read returns retained immutable state; coordination/acceptance have no age expiry.
func (s *Store) Read(ctx context.Context, key string, now time.Time) (Object, error) {
	if !validKey(key) {
		return Object{}, fmt.Errorf("invalid state key %q", key)
	}
	object, err := s.objects.Get(ctx, key)
	if err != nil {
		return Object{}, err
	}
	retained := strings.HasPrefix(key, "coordination/") || strings.HasPrefix(key, "acceptance/")
	if !retained && !object.Modified.Add(30*24*time.Hour).After(now) {
		return Object{}, ErrExpired
	}
	return object, nil
}

// CompareAndSwap updates coordination only; a conflict requires fresh caller evaluation.
func (s *Store) CompareAndSwap(ctx context.Context, key string, data []byte, etag string) (string, error) {
	if !validCoordinationKey(key) {
		return "", fmt.Errorf("invalid coordination key %q", key)
	}
	return s.objects.Put(ctx, key, data, etag)
}

// validCoordinationKey confines each execution document to the coordination root.
func validCoordinationKey(key string) bool {
	return validKey(key) && strings.HasPrefix(key, "coordination/") && strings.HasSuffix(key, ".json") && !strings.Contains(strings.TrimPrefix(key, "coordination/"), "/")
}

// Scan fetches one bounded prefix page without mutating or repairing stored state.
func (s *Store) Scan(ctx context.Context, prefix, token string, limit int32) (Page, error) {
	if !strings.HasSuffix(prefix, "/") || !validKey(strings.TrimSuffix(prefix, "/")) || limit < 1 || limit > 1000 {
		return Page{}, fmt.Errorf("invalid scan prefix or limit")
	}
	return s.objects.List(ctx, prefix, token, limit)
}

// validKey rejects ambiguous path segments and control characters in state keys.
func validKey(key string) bool {
	return key != "" && key != "." && key != ".." && !strings.HasPrefix(key, "/") && !strings.HasPrefix(key, "../") && path.Clean(key) == key && !strings.Contains(key, "\\") && strings.IndexFunc(key, unicode.IsControl) < 0
}
