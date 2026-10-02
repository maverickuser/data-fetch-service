package state

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// memoryObjects models atomic conditions with monotonically changing opaque ETags.
type memoryObjects struct {
	mu                                     sync.Mutex
	objects                                map[string]Object
	version                                int
	now                                    time.Time
	putErr, errorAfterPut, getErr, listErr error
}

func newMemory() *memoryObjects {
	return &memoryObjects{objects: map[string]Object{}, now: time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC)}
}
func (m *memoryObjects) Get(_ context.Context, key string) (Object, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getErr != nil {
		return Object{}, m.getErr
	}
	o, ok := m.objects[key]
	if !ok {
		return Object{}, ErrNotFound
	}
	o.Data = append([]byte(nil), o.Data...)
	return o, nil
}
func (m *memoryObjects) Put(_ context.Context, key string, data []byte, match string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.putErr != nil {
		return "", m.putErr
	}
	o, exists := m.objects[key]
	if (match == "" && exists) || (match != "" && (!exists || o.ETag != match)) {
		return "", ErrConflict
	}
	m.version++
	etag := fmt.Sprintf("opaque-%d", m.version)
	m.objects[key] = Object{append([]byte(nil), data...), etag, m.now}
	return etag, m.errorAfterPut
}
func (m *memoryObjects) List(_ context.Context, prefix, token string, limit int32) (Page, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.listErr != nil {
		return Page{}, m.listErr
	}
	var keys []string
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) && k > token {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	page := Page{Keys: keys}
	if len(keys) > int(limit) {
		page.Keys = keys[:limit]
		page.NextToken = page.Keys[len(page.Keys)-1]
	}
	return page, nil
}

func TestConcurrentImmutableWritersAndLostResponse(t *testing.T) {
	m := newMemory()
	s := New(m)
	ctx := context.Background()
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			if err := s.Create(ctx, "runs/r/snapshot.json", []byte(`{"run":"r"}`)); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if err := s.Create(ctx, "runs/r/snapshot.json", []byte(`{"run":"other"}`)); !errors.Is(err, ErrIntegrity) {
		t.Fatal(err)
	}
	m.errorAfterPut = errors.New("response lost")
	if err := s.Create(ctx, "runs/r/history/1.json", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	m.getErr = errors.New("read unavailable")
	if err := s.Create(ctx, "runs/r/history/2.json", []byte(`{}`)); !errors.Is(err, m.getErr) {
		t.Fatal(err)
	}
	m.getErr = nil
	m.putErr = errors.New("write unavailable")
	if err := s.Create(ctx, "runs/r/history/3.json", nil); !errors.Is(err, m.putErr) {
		t.Fatal(err)
	}
}

func TestCoordinationCASRejectsStaleETag(t *testing.T) {
	m := newMemory()
	s := New(m)
	ctx := context.Background()
	key := "coordination/execution.json"
	etag, err := s.CompareAndSwap(ctx, key, []byte(`{"generation":1}`), "")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for range 2 {
		wg.Go(func() { _, err := s.CompareAndSwap(ctx, key, []byte(`{"generation":2}`), etag); results <- err })
	}
	wg.Wait()
	close(results)
	successes, conflicts := 0, 0
	for err := range results {
		if err == nil {
			successes++
		} else if errors.Is(err, ErrConflict) {
			conflicts++
		} else {
			t.Fatal(err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatal(successes, conflicts)
	}
	if err := s.Create(ctx, key, nil); err == nil {
		t.Fatal("immutable overwrite of coordination allowed")
	}
	if _, err := s.CompareAndSwap(ctx, "runs/r/snapshot.json", nil, ""); err == nil {
		t.Fatal("mutable run allowed")
	}
}

func TestRetentionAndBoundedScan(t *testing.T) {
	m := newMemory()
	s := New(m)
	ctx := context.Background()
	for _, key := range []string{"runs/r/snapshot.json", "runs/r/history/1.json", "acceptance/k/r.json"} {
		if err := s.Create(ctx, key, []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Read(ctx, "runs/r/snapshot.json", m.now.Add(30*24*time.Hour-time.Nanosecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Read(ctx, "runs/r/snapshot.json", m.now.Add(30*24*time.Hour)); !errors.Is(err, ErrExpired) {
		t.Fatal(err)
	}
	if _, err := s.Read(ctx, "acceptance/k/r.json", m.now.Add(365*24*time.Hour)); err != nil {
		t.Fatal("baseline expired", err)
	}
	if _, err := s.Read(ctx, "missing", m.now); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	first, err := s.Scan(ctx, "runs/r/", "", 1)
	if err != nil || len(first.Keys) != 1 || first.NextToken == "" {
		t.Fatal(first, err)
	}
	second, err := s.Scan(ctx, "runs/r/", first.NextToken, 1)
	if err != nil || len(second.Keys) != 1 || second.Keys[0] == first.Keys[0] || second.NextToken != "" {
		t.Fatal(second, err)
	}
	m.listErr = errors.New("list unavailable")
	if _, err := s.Scan(ctx, "runs/", "", 1); !errors.Is(err, m.listErr) {
		t.Fatal(err)
	}
}

func TestInvalidPathsAndScanBudgets(t *testing.T) {
	s := New(newMemory())
	ctx := context.Background()
	for _, key := range []string{"", ".", "..", "../x", "/x", "a/../x", "a\\x", "x\t"} {
		if err := s.Create(ctx, key, nil); err == nil {
			t.Fatal(key)
		}
		if _, err := s.Read(ctx, key, time.Now()); err == nil {
			t.Fatal(key)
		}
	}
	for _, tc := range []struct {
		prefix string
		limit  int32
	}{{"runs", 1}, {"../", 1}, {"runs/", 0}, {"runs/", 1001}} {
		if _, err := s.Scan(ctx, tc.prefix, "", tc.limit); err == nil {
			t.Fatal(tc)
		}
	}
}
