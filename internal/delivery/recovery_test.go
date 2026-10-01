package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/maverickuser/data-fetch-service/internal/domain"
	"github.com/maverickuser/data-fetch-service/internal/state"
)

type recoveryObjects struct {
	items     map[string]state.Object
	writes    map[string]int
	failKey   string
	failWrite int
	nextETag  int
}

func (o *recoveryObjects) Get(_ context.Context, key string) (state.Object, error) {
	item, ok := o.items[key]
	if !ok {
		return state.Object{}, state.ErrNotFound
	}
	return item, nil
}
func (o *recoveryObjects) Put(_ context.Context, key string, data []byte, match string) (string, error) {
	o.writes[key]++
	if key == o.failKey && o.writes[key] == o.failWrite {
		return "", errors.New("injected storage failure")
	}
	old, exists := o.items[key]
	if match == "" && exists || match != "" && (!exists || old.ETag != match) {
		return "", state.ErrConflict
	}
	o.nextETag++
	etag := fmt.Sprintf("etag-%d", o.nextETag)
	o.items[key] = state.Object{Data: append([]byte(nil), data...), ETag: etag, Modified: fixedTime}
	return etag, nil
}
func (*recoveryObjects) List(context.Context, string, string, int32) (state.Page, error) {
	return state.Page{}, nil
}

func realRecoveryFixture(t *testing.T) (*Service, *recoveryObjects, *state.Coordinator) {
	t.Helper()
	base, repo, _, raw := fixture(t, true)
	objects := &recoveryObjects{items: map[string]state.Object{}, writes: map[string]int{}}
	for key, data := range repo.objects {
		objects.items[key] = state.Object{Data: data, ETag: "initial", Modified: fixedTime}
	}
	coord := state.Coordination{SchemaVersion: 1, Generation: 0, ActiveRunID: "run_1", Phase: domain.DeliveryPending, LastSequence: 4, Dispatch: &state.Dispatch{RunID: "run_1", Queue: "delivery", State: "sent"}}
	encoded, err := json.Marshal(coord)
	if err != nil {
		t.Fatal(err)
	}
	objects.items["coordination/exec1.json"] = state.Object{Data: encoded, ETag: "initial", Modified: fixedTime}
	store := state.New(objects)
	coordinator := state.NewCoordinator(store, func() time.Time { return fixedTime })
	base.Repository = store
	base.Coordinator = coordinator
	base.Manifests = memoryManifest{data: raw}
	base.Client = clientFunc(func(*http.Request) (*http.Response, error) { return acceptedResponse("run_1"), nil })
	return base, objects, coordinator
}

func TestRealCoordinatorRepairsAcceptedWriteBoundaries(t *testing.T) {
	cases := []struct {
		key string
		n   int
	}{
		{"runs/run_1/delivery/accepted.json", 1},
		{"coordination/exec1.json", 4},
		{"acceptance/exec1/run_1.json", 1},
		{"runs/run_1/history/00000000000000000006.json", 1},
		{"coordination/exec1.json", 5},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s-%d", tc.key, tc.n), func(t *testing.T) {
			svc, objects, coord := realRecoveryFixture(t)
			calls := 0
			svc.Client = clientFunc(func(*http.Request) (*http.Response, error) { calls++; return acceptedResponse("run_1"), nil })
			objects.failKey, objects.failWrite = tc.key, tc.n
			if err := svc.Run(context.Background(), "run_1", "token", fixedTime.Add(time.Minute)); err == nil {
				t.Fatal("expected injected error")
			}
			before, _, err := coord.Load(context.Background(), "coordination/exec1.json")
			if err != nil || before.AcceptedBaseline != nil {
				t.Fatal(err, before)
			}
			objects.failKey = ""
			lease := state.Lease{RunID: "run_1", Generation: before.Generation, Token: "token"}
			if err := svc.RecoverAccepted(context.Background(), "run_1", lease); err != nil {
				t.Fatal(err)
			}
			after, _, err := coord.Load(context.Background(), "coordination/exec1.json")
			if err != nil || after.Phase != domain.Completed || after.AcceptedBaseline == nil || after.AcceptedBaseline.Fingerprint != fingerprint || calls != 1 {
				t.Fatal(err, after, calls)
			}
			if _, err := objects.Get(context.Background(), "acceptance/exec1/run_1.json"); err != nil {
				t.Fatal(err)
			}
			if err := svc.RecoverAccepted(context.Background(), "run_1", lease); err != nil {
				t.Fatal("idempotent accepted recovery", err)
			}
		})
	}
}

func TestFinishedReceiptRecoversAfterAcceptedRecordFailure(t *testing.T) {
	svc, objects, coord := realRecoveryFixture(t)
	objects.failKey, objects.failWrite = "runs/run_1/delivery/accepted.json", 1
	if err := svc.Run(context.Background(), "run_1", "token", fixedTime.Add(time.Minute)); err == nil {
		t.Fatal("expected accepted write error")
	}
	objects.failKey = ""
	before, _, err := coord.Load(context.Background(), "coordination/exec1.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.RecoverAccepted(context.Background(), "run_1", state.Lease{RunID: "run_1", Generation: before.Generation, Token: "token"}); err != nil {
		t.Fatal(err)
	}
	after, _, err := coord.Load(context.Background(), "coordination/exec1.json")
	if err != nil || after.Phase != domain.Completed || after.AcceptedBaseline == nil {
		t.Fatal(err, after)
	}
}

func TestLostFinishedWriteNeedsNextBudgetedHTTPAttempt(t *testing.T) {
	svc, objects, coord := realRecoveryFixture(t)
	objects.failKey, objects.failWrite = "runs/run_1/delivery/1/finished.json", 1
	calls := 0
	svc.Client = clientFunc(func(*http.Request) (*http.Response, error) { calls++; return acceptedResponse("run_1"), nil })
	if err := svc.Run(context.Background(), "run_1", "token", fixedTime.Add(time.Minute)); err == nil {
		t.Fatal("expected lost finished write")
	}
	objects.failKey = ""
	current, _, err := coord.Load(context.Background(), "coordination/exec1.json")
	if err != nil || current.AcceptedBaseline != nil || current.Phase != domain.Delivering || calls != 1 {
		t.Fatal(err, current, calls)
	}
	lease := state.Lease{RunID: "run_1", Generation: current.Generation, Token: "token"}
	if err := svc.RecoverAccepted(context.Background(), "run_1", lease); !errors.Is(err, state.ErrNotFound) {
		t.Fatal("unrecorded 202 cannot be treated as accepted", err)
	}
}
