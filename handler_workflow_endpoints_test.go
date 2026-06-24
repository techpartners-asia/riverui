package riverui

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivertype"
)

func insertWorkflowJobForTest(ctx context.Context, t *testing.T, bundle *setupEndpointTestBundle, workflowID, workflowName, taskName string, deps []string, state rivertype.JobState) *rivertype.JobRow {
	t.Helper()

	meta := map[string]any{
		metadataKeyWorkflowID:   workflowID,
		metadataKeyWorkflowName: workflowName,
		metadataKeyWorkflowTask: taskName,
	}
	if len(deps) > 0 {
		meta[metadataKeyWorkflowDeps] = deps
	}
	metaBytes, err := json.Marshal(meta)
	require.NoError(t, err)

	var finalizedAt *time.Time
	if state == rivertype.JobStateCompleted || state == rivertype.JobStateCancelled || state == rivertype.JobStateDiscarded {
		ft := time.Now()
		finalizedAt = &ft
	}

	row, err := bundle.exec.JobInsertFull(ctx, &riverdriver.JobInsertFullParams{
		EncodedArgs: []byte(`{}`),
		FinalizedAt: finalizedAt,
		Kind:        "test_workflow",
		MaxAttempts: 3,
		Metadata:    metaBytes,
		Priority:    1,
		Queue:       "default",
		ScheduledAt: ptrutil.Ptr(time.Now()),
		State:       state,
		Tags:        []string{},
	})
	require.NoError(t, err)
	_ = pgtype.Text{}
	_ = ptrutil.Ptr("")
	return row
}

func TestAPIWorkflowGetEndpoint(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	endpoint, bundle := setupEndpoint(ctx, t, newWorkflowGetEndpoint)

	workflowID := "wf-render-test"
	a := insertWorkflowJobForTest(ctx, t, bundle, workflowID, "render-test", "a", nil, rivertype.JobStateCompleted)
	b := insertWorkflowJobForTest(ctx, t, bundle, workflowID, "render-test", "b", []string{"a"}, rivertype.JobStatePending)

	resp, err := endpoint.Execute(ctx, &workflowGetRequest{ID: workflowID})
	require.NoError(t, err)
	require.Equal(t, workflowID, resp.ID)
	require.Equal(t, "render-test", resp.Name)
	require.Len(t, resp.Tasks, 2)

	// Ordered by job ID (insertion order).
	require.Equal(t, a.ID, resp.Tasks[0].ID)
	require.Equal(t, "a", resp.Tasks[0].Name)
	require.Empty(t, resp.Tasks[0].Deps)
	require.Equal(t, "completed", resp.Tasks[0].State)

	require.Equal(t, b.ID, resp.Tasks[1].ID)
	require.Equal(t, "b", resp.Tasks[1].Name)
	require.Equal(t, []string{"a"}, resp.Tasks[1].Deps)
	require.Equal(t, "pending", resp.Tasks[1].State)
	require.Equal(t, workflowID, resp.Tasks[1].WorkflowID)
}

func TestAPIWorkflowGetEndpoint_NotFound(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	endpoint, _ := setupEndpoint(ctx, t, newWorkflowGetEndpoint)

	_, err := endpoint.Execute(ctx, &workflowGetRequest{ID: "does-not-exist"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Workflow not found")
}

func TestAPIWorkflowCancelEndpoint(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	endpoint, bundle := setupEndpoint(ctx, t, newWorkflowCancelEndpoint)

	workflowID := "wf-cancel-api"
	_ = insertWorkflowJobForTest(ctx, t, bundle, workflowID, "cancel-test", "a", nil, rivertype.JobStateCompleted)
	pending := insertWorkflowJobForTest(ctx, t, bundle, workflowID, "cancel-test", "b", []string{"a"}, rivertype.JobStatePending)

	resp, err := endpoint.Execute(ctx, &workflowCancelRequest{ID: workflowID})
	require.NoError(t, err)
	require.Len(t, resp.CancelledJobs, 1)
	require.Equal(t, pending.ID, resp.CancelledJobs[0].ID)
	require.Equal(t, "cancelled", resp.CancelledJobs[0].State)
}

func TestAPIWorkflowCancelEndpoint_LeavesRunningTasksRunning(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	endpoint, bundle := setupEndpoint(ctx, t, newWorkflowCancelEndpoint)

	workflowID := "wf-cancel-running-api"
	running := insertWorkflowJobForTest(ctx, t, bundle, workflowID, "running-test", "a", nil, rivertype.JobStateRunning)

	resp, err := endpoint.Execute(ctx, &workflowCancelRequest{ID: workflowID})
	require.NoError(t, err)
	require.Len(t, resp.CancelledJobs, 1)
	require.Equal(t, running.ID, resp.CancelledJobs[0].ID)
	require.Equal(t, "running", resp.CancelledJobs[0].State, "running task must stay in running state")
}

func TestAPIWorkflowGetEndpoint_DepsSerializedAsArray(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	endpoint, bundle := setupEndpoint(ctx, t, newWorkflowGetEndpoint)

	workflowID := "wf-deps-empty"
	_ = insertWorkflowJobForTest(ctx, t, bundle, workflowID, "deps-test", "a", nil, rivertype.JobStateCompleted)

	resp, err := endpoint.Execute(ctx, &workflowGetRequest{ID: workflowID})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 1)
	require.NotNil(t, resp.Tasks[0].Deps, "Deps must never be nil (serializes to JSON null, breaks frontend)")
	require.Equal(t, []string{}, resp.Tasks[0].Deps)

	// Marshal to JSON and verify "deps":[] not "deps":null.
	b, err := json.Marshal(resp)
	require.NoError(t, err)
	require.Contains(t, string(b), `"deps":[]`)
	require.NotContains(t, string(b), `"deps":null`)
}

func TestAPIWorkflowListEndpoint(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	endpoint, bundle := setupEndpoint(ctx, t, newWorkflowListEndpoint)

	// Insert two workflows with distinct IDs.
	_ = insertWorkflowJobForTest(ctx, t, bundle, "wf-list-a", "alpha", "step1", nil, rivertype.JobStatePending)
	_ = insertWorkflowJobForTest(ctx, t, bundle, "wf-list-a", "alpha", "step2", []string{"step1"}, rivertype.JobStatePending)
	_ = insertWorkflowJobForTest(ctx, t, bundle, "wf-list-b", "beta", "only", nil, rivertype.JobStateCompleted)

	// List all.
	resp, err := endpoint.Execute(ctx, &workflowListRequest{})
	require.NoError(t, err)
	ids := make([]string, len(resp.Data))
	for i, w := range resp.Data {
		ids[i] = w.ID
	}
	require.Contains(t, ids, "wf-list-a")
	require.Contains(t, ids, "wf-list-b")

	// Filter active only — wf-list-a has pending tasks.
	activeResp, err := endpoint.Execute(ctx, &workflowListRequest{State: "active"})
	require.NoError(t, err)
	activeIDs := make([]string, len(activeResp.Data))
	for i, w := range activeResp.Data {
		activeIDs[i] = w.ID
	}
	require.Contains(t, activeIDs, "wf-list-a", "wf-list-a has pending tasks; should be active")
	require.NotContains(t, activeIDs, "wf-list-b", "wf-list-b is all completed; should be inactive")
}

// TestAPIWorkflowGetEndpoint_MetadataContract pins the wire-level shape of
// task metadata so the frontend's lookup of metadata["river:workflow_id"] can
// never silently regress again. The frontend derives the workflow ID from
// this exact key when wiring the retry and cancel buttons; an undefined here
// makes the buttons silently no-op (no toast, no error, no network request).
func TestAPIWorkflowGetEndpoint_MetadataContract(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	endpoint, bundle := setupEndpoint(ctx, t, newWorkflowGetEndpoint)

	workflowID := "wf-contract"
	_ = insertWorkflowJobForTest(ctx, t, bundle, workflowID, "contract-test", "a", nil, rivertype.JobStateCompleted)
	_ = insertWorkflowJobForTest(ctx, t, bundle, workflowID, "contract-test", "b", []string{"a"}, rivertype.JobStatePending)

	resp, err := endpoint.Execute(ctx, &workflowGetRequest{ID: workflowID})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 2)

	// Round-trip through JSON the same way the frontend will receive it.
	wireBytes, err := json.Marshal(resp)
	require.NoError(t, err)

	var wire struct {
		Tasks []struct {
			Metadata map[string]json.RawMessage `json:"metadata"`
		} `json:"tasks"`
	}
	require.NoError(t, json.Unmarshal(wireBytes, &wire))
	require.Len(t, wire.Tasks, 2)

	for i, task := range wire.Tasks {
		var gotID, gotTask, gotName string
		var gotDeps []string

		raw, ok := task.Metadata["river:workflow_id"]
		require.Truef(t, ok, "task %d: metadata missing river:workflow_id", i)
		require.NoError(t, json.Unmarshal(raw, &gotID))
		require.Equalf(t, workflowID, gotID, "task %d: river:workflow_id value mismatch", i)

		raw, ok = task.Metadata["river:workflow_task"]
		require.Truef(t, ok, "task %d: metadata missing river:workflow_task", i)
		require.NoError(t, json.Unmarshal(raw, &gotTask))
		require.NotEmptyf(t, gotTask, "task %d: river:workflow_task empty", i)

		raw, ok = task.Metadata["river:workflow_name"]
		require.Truef(t, ok, "task %d: metadata missing river:workflow_name", i)
		require.NoError(t, json.Unmarshal(raw, &gotName))
		require.Equalf(t, "contract-test", gotName, "task %d: river:workflow_name value mismatch", i)

		if raw, ok := task.Metadata["river:workflow_deps"]; ok {
			require.NoError(t, json.Unmarshal(raw, &gotDeps))
		}
		if i == 1 {
			require.Equalf(t, []string{"a"}, gotDeps, "task %d: river:workflow_deps value mismatch", i)
		}
	}
}

// TestAPIWorkflowGetEndpoint_WaitReason verifies that pending tasks with an
// incomplete dep report "dependencies", and that non-pending tasks always
// report "none". A dep is considered incomplete when its sibling has not yet
// reached a satisfied terminal state (completed, or cancelled/discarded when
// the matching ignore flag is set).
func TestAPIWorkflowGetEndpoint_WaitReason(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	endpoint, bundle := setupEndpoint(ctx, t, newWorkflowGetEndpoint)

	workflowID := "wf-wait-reason"
	// "a" is running — still an incomplete dep.
	_ = insertWorkflowJobForTest(ctx, t, bundle, workflowID, "wait-test", "a", nil, rivertype.JobStateRunning)
	// "b" is pending waiting on the still-running "a" → "dependencies".
	_ = insertWorkflowJobForTest(ctx, t, bundle, workflowID, "wait-test", "b", []string{"a"}, rivertype.JobStatePending)
	// "c" is completed — no longer blocking anything.
	_ = insertWorkflowJobForTest(ctx, t, bundle, workflowID, "wait-test", "c", nil, rivertype.JobStateCompleted)
	// "d" is pending but its dep "c" is already completed → no incomplete deps → "none".
	_ = insertWorkflowJobForTest(ctx, t, bundle, workflowID, "wait-test", "d", []string{"c"}, rivertype.JobStatePending)
	// "e" is cancelled — always "none".
	_ = insertWorkflowJobForTest(ctx, t, bundle, workflowID, "wait-test", "e", nil, rivertype.JobStateCancelled)

	resp, err := endpoint.Execute(ctx, &workflowGetRequest{ID: workflowID})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 5)

	byName := map[string]string{}
	for _, task := range resp.Tasks {
		byName[task.Name] = task.WaitReason
	}
	require.Equal(t, "none", byName["a"], "running task should not be waiting")
	require.Equal(t, "dependencies", byName["b"], "pending task with incomplete dep should be blocked by dependencies")
	require.Equal(t, "none", byName["c"], "completed task should not be waiting")
	require.Equal(t, "none", byName["d"], "pending task whose dep is completed has no incomplete deps")
	require.Equal(t, "none", byName["e"], "cancelled task should not be waiting")
}

// TestAPIWorkflowListEndpoint_CountFailedDepsDistinguishesCascade pins the
// distinction between user-initiated cancellations and cascade failures
// (cancelled tasks whose dep is itself cancelled or discarded). Without it,
// the workflow list page shows count_failed_deps=0 forever and operators
// can't tell at a glance which workflows have a real failure to investigate.
func TestAPIWorkflowListEndpoint_CountFailedDepsDistinguishesCascade(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	endpoint, bundle := setupEndpoint(ctx, t, newWorkflowListEndpoint)

	// Workflow A: a real cascade — `charge` is discarded, `notify` is cancelled
	// because its dep `charge` failed.
	cascadeWF := "wf-cascade"
	_ = insertWorkflowJobForTest(ctx, t, bundle, cascadeWF, "cascade-test", "ingest", nil, rivertype.JobStateCompleted)
	_ = insertWorkflowJobForTest(ctx, t, bundle, cascadeWF, "cascade-test", "charge", []string{"ingest"}, rivertype.JobStateDiscarded)
	_ = insertWorkflowJobForTest(ctx, t, bundle, cascadeWF, "cascade-test", "notify", []string{"charge"}, rivertype.JobStateCancelled)

	// Workflow B: user-cancelled tasks with no failed deps.
	userWF := "wf-user-cancel"
	_ = insertWorkflowJobForTest(ctx, t, bundle, userWF, "user-cancel-test", "a", nil, rivertype.JobStateCancelled)
	_ = insertWorkflowJobForTest(ctx, t, bundle, userWF, "user-cancel-test", "b", nil, rivertype.JobStateCancelled)

	resp, err := endpoint.Execute(ctx, &workflowListRequest{})
	require.NoError(t, err)

	byID := map[string]*workflowListItem{}
	for _, w := range resp.Data {
		byID[w.ID] = w
	}

	cascade, ok := byID[cascadeWF]
	require.True(t, ok, "cascade workflow missing from list")
	require.Equal(t, 1, cascade.CountFailedDeps, "notify task should be cascade-failed via charge")
	require.Equal(t, 0, cascade.CountCancelled, "cascade-failed tasks must not double-count under cancelled")
	require.Equal(t, 1, cascade.CountDiscarded, "charge was discarded directly")

	user, ok := byID[userWF]
	require.True(t, ok, "user-cancel workflow missing from list")
	require.Equal(t, 0, user.CountFailedDeps, "user-cancelled tasks have no failed deps")
	require.Equal(t, 2, user.CountCancelled, "both user-cancelled tasks should stay in cancelled bucket")
}

// TestAPIWorkflowRerunEndpoint verifies that rerunning a workflow creates
// a fresh set of tasks under a new workflow_id, preserving the original
// DAG structure (deps), kind, args, queue, and other shape; while the
// original workflow's rows are untouched.
func TestAPIWorkflowRerunEndpoint(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	endpoint, bundle := setupEndpoint(ctx, t, newWorkflowRerunEndpoint)

	origID := "wf-rerun-source"
	_ = insertWorkflowJobForTest(ctx, t, bundle, origID, "billing", "ingest", nil, rivertype.JobStateCompleted)
	_ = insertWorkflowJobForTest(ctx, t, bundle, origID, "billing", "charge", []string{"ingest"}, rivertype.JobStateCompleted)
	_ = insertWorkflowJobForTest(ctx, t, bundle, origID, "billing", "notify", []string{"charge"}, rivertype.JobStateCompleted)

	resp, err := endpoint.Execute(ctx, &workflowRerunRequest{ID: origID})
	require.NoError(t, err)
	require.NotEmpty(t, resp.WorkflowID)
	require.NotEqual(t, origID, resp.WorkflowID, "new workflow must have a different id")
	require.Len(t, resp.InsertedJobs, 3)

	// Inspect the new workflow's tasks via JobGetWorkflowTasks. They must
	// have the same kind, args, and deps as the originals — but be fresh
	// (state available or pending, no errors, attempt=0).
	newRows, err := bundle.exec.JobGetWorkflowTasks(ctx, &riverdriver.JobGetWorkflowTasksParams{
		WorkflowID: resp.WorkflowID,
	})
	require.NoError(t, err)
	require.Len(t, newRows, 3, "new workflow should have 3 tasks")

	byName := map[string]*rivertype.JobRow{}
	for _, r := range newRows {
		var meta map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(r.Metadata, &meta))
		var name string
		require.NoError(t, json.Unmarshal(meta[metadataKeyWorkflowTask], &name))
		byName[name] = r
	}

	ingest := byName["ingest"]
	require.NotNil(t, ingest)
	require.Equal(t, "test_workflow", ingest.Kind)
	require.Equal(t, rivertype.JobStateAvailable, ingest.State, "no-deps task starts available")
	require.Equal(t, 0, ingest.Attempt)

	charge := byName["charge"]
	require.NotNil(t, charge)
	require.Equal(t, rivertype.JobStatePending, charge.State, "deps-task starts pending")

	notify := byName["notify"]
	require.NotNil(t, notify)
	require.Equal(t, rivertype.JobStatePending, notify.State)

	// Original tasks must be untouched.
	origAfter, err := bundle.exec.JobGetWorkflowTasks(ctx, &riverdriver.JobGetWorkflowTasksParams{
		WorkflowID: origID,
	})
	require.NoError(t, err)
	require.Len(t, origAfter, 3)
	for _, r := range origAfter {
		require.Equal(t, rivertype.JobStateCompleted, r.State, "original workflow rows must not be modified")
	}
}

func TestAPIWorkflowRerunEndpoint_NotFound(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	endpoint, _ := setupEndpoint(ctx, t, newWorkflowRerunEndpoint)

	_, err := endpoint.Execute(ctx, &workflowRerunRequest{ID: "nope"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Workflow not found")
}

func TestAPIWorkflowRetryEndpoint(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	endpoint, bundle := setupEndpoint(ctx, t, newWorkflowRetryEndpoint)

	workflowID := "wf-retry-api"
	_ = insertWorkflowJobForTest(ctx, t, bundle, workflowID, "retry-test", "a", nil, rivertype.JobStateDiscarded)
	_ = insertWorkflowJobForTest(ctx, t, bundle, workflowID, "retry-test", "b", []string{"a"}, rivertype.JobStateCancelled)

	resp, err := endpoint.Execute(ctx, &workflowRetryRequest{ID: workflowID, Mode: "failed_and_downstream"})
	require.NoError(t, err)
	require.Len(t, resp.RetriedJobs, 2)
	for _, j := range resp.RetriedJobs {
		require.NotEqual(t, "cancelled", j.State)
		require.NotEqual(t, "discarded", j.State)
	}
}

// insertWorkflowWaitJobForTest inserts a workflow task that carries a
// river:workflow_wait metadata key. The waitSpec is the raw JSON for the
// WaitSpec; startedAt and resolvedAt are optional RFC3339 strings (pass ""
// to omit).
func insertWorkflowWaitJobForTest(
	ctx context.Context,
	t *testing.T,
	bundle *setupEndpointTestBundle,
	workflowID, workflowName, taskName string,
	deps []string,
	state rivertype.JobState,
	waitSpecJSON string,
	startedAt, resolvedAt string,
) *rivertype.JobRow {
	t.Helper()

	meta := map[string]any{
		metadataKeyWorkflowID:   workflowID,
		metadataKeyWorkflowName: workflowName,
		metadataKeyWorkflowTask: taskName,
	}
	if len(deps) > 0 {
		meta[metadataKeyWorkflowDeps] = deps
	}
	meta[metadataKeyWorkflowWait] = json.RawMessage(waitSpecJSON)
	if startedAt != "" {
		meta[metadataKeyWorkflowWaitStartedAt] = startedAt
	}
	if resolvedAt != "" {
		meta[metadataKeyWorkflowWaitResolvedAt] = resolvedAt
	}
	metaBytes, err := json.Marshal(meta)
	require.NoError(t, err)

	var finalizedAt *time.Time
	if state == rivertype.JobStateCompleted || state == rivertype.JobStateCancelled || state == rivertype.JobStateDiscarded {
		ft := time.Now()
		finalizedAt = &ft
	}

	row, err := bundle.exec.JobInsertFull(ctx, &riverdriver.JobInsertFullParams{
		EncodedArgs: []byte(`{}`),
		FinalizedAt: finalizedAt,
		Kind:        "test_workflow",
		MaxAttempts: 3,
		Metadata:    metaBytes,
		Priority:    1,
		Queue:       "default",
		ScheduledAt: ptrutil.Ptr(time.Now()),
		State:       state,
		Tags:        []string{},
	})
	require.NoError(t, err)
	return row
}

// TestAPIWorkflowGetEndpoint_WaitObject asserts that the serialized `wait`
// object has the correct shape for signal and timer terms, and that
// wait_reason is computed correctly across the four meaningful scenarios:
// (1) pending wait-only, (2) pending wait+incomplete dep, (3) resolved wait,
// (4) non-pending task.
func TestAPIWorkflowGetEndpoint_WaitObject(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	endpoint, bundle := setupEndpoint(ctx, t, newWorkflowGetEndpoint)

	workflowID := "wf-wait-obj"

	// Signal-term wait spec.
	signalSpec := `{"expr":"signal_received","terms":[{"name":"sig1","kind":"signal","key":"my-signal","label":"My Signal"}]}`
	// Timer-term wait spec.
	timerSpec := `{"expr":"timer_fired","terms":[{"name":"tim1","kind":"timer","label":"My Timer","timer":{"name":"t1","kind":"absolute"}}]}`
	// Generic-term wait spec (has cel_expr, no signal_key/timer_name).
	genericSpec := `{"expr":"custom","terms":[{"name":"gen1","kind":"generic","label":"","cel_expr":"x > 0"}]}`

	startedAt := "2026-06-23T10:00:00Z"
	resolvedAt := "2026-06-23T11:00:00Z"

	// (1) Pending signal wait, no deps → wait_reason = "wait".
	_ = insertWorkflowWaitJobForTest(ctx, t, bundle, workflowID, "wait-obj-test", "signal-wait",
		nil, rivertype.JobStatePending, signalSpec, startedAt, "")

	// (2) Pending wait + a dep on a running task → wait_reason = "dependencies_and_wait".
	_ = insertWorkflowJobForTest(ctx, t, bundle, workflowID, "wait-obj-test", "runner", nil, rivertype.JobStateRunning)
	_ = insertWorkflowWaitJobForTest(ctx, t, bundle, workflowID, "wait-obj-test", "wait-and-dep",
		[]string{"runner"}, rivertype.JobStatePending, timerSpec, "", "")

	// (3) Task whose wait has resolved → wait_reason = "none".
	_ = insertWorkflowWaitJobForTest(ctx, t, bundle, workflowID, "wait-obj-test", "resolved-wait",
		nil, rivertype.JobStateCompleted, signalSpec, startedAt, resolvedAt)

	// (4) Generic wait, no label (should fall back to name), cel_expr present.
	_ = insertWorkflowWaitJobForTest(ctx, t, bundle, workflowID, "wait-obj-test", "generic-wait",
		nil, rivertype.JobStatePending, genericSpec, "", "")

	resp, err := endpoint.Execute(ctx, &workflowGetRequest{ID: workflowID})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 5)

	byName := map[string]*workflowTaskSerializable{}
	for _, task := range resp.Tasks {
		byName[task.Name] = task
	}

	// --- (1) signal-wait ---
	sw := byName["signal-wait"]
	require.NotNil(t, sw, "signal-wait task must be present")
	require.NotNil(t, sw.Wait, "signal-wait must have a wait object")
	require.Equal(t, "signal_received", sw.Wait.ExprCEL)
	require.Equal(t, "waiting", sw.Wait.Phase, "started_at present + pending → 'waiting'")
	require.Equal(t, &startedAt, sw.Wait.StartedAt)
	require.Nil(t, sw.Wait.ResolvedAt)
	require.Len(t, sw.Wait.Terms, 1)
	require.Equal(t, "sig1", sw.Wait.Terms[0].Name)
	require.Equal(t, "signal", sw.Wait.Terms[0].Kind)
	require.Equal(t, "My Signal", sw.Wait.Terms[0].Label)
	require.NotNil(t, sw.Wait.Terms[0].SignalKey)
	require.Equal(t, "my-signal", *sw.Wait.Terms[0].SignalKey)
	require.Nil(t, sw.Wait.Terms[0].TimerName)
	// inputs must be non-nil empty arrays.
	require.NotNil(t, sw.Wait.Inputs.Deps)
	require.NotNil(t, sw.Wait.Inputs.Signals)
	require.NotNil(t, sw.Wait.Inputs.Timers)
	require.Equal(t, "wait", sw.WaitReason)

	// --- (2) wait-and-dep ---
	wd := byName["wait-and-dep"]
	require.NotNil(t, wd, "wait-and-dep task must be present")
	require.NotNil(t, wd.Wait, "wait-and-dep must have a wait object")
	require.Equal(t, "timer_fired", wd.Wait.ExprCEL)
	require.Equal(t, "not_started", wd.Wait.Phase, "no started_at → 'not_started'")
	require.Len(t, wd.Wait.Terms, 1)
	require.Equal(t, "timer", wd.Wait.Terms[0].Kind)
	require.NotNil(t, wd.Wait.Terms[0].TimerName)
	require.Equal(t, "t1", *wd.Wait.Terms[0].TimerName)
	require.Nil(t, wd.Wait.Terms[0].SignalKey)
	require.Equal(t, "dependencies_and_wait", wd.WaitReason)

	// --- (3) resolved-wait ---
	rw := byName["resolved-wait"]
	require.NotNil(t, rw, "resolved-wait task must be present")
	require.NotNil(t, rw.Wait, "resolved-wait must have a wait object")
	require.Equal(t, "resolved", rw.Wait.Phase)
	require.Equal(t, &resolvedAt, rw.Wait.ResolvedAt)
	require.Equal(t, "none", rw.WaitReason, "resolved wait → not blocking")

	// --- (4) generic-wait label fallback ---
	gw := byName["generic-wait"]
	require.NotNil(t, gw, "generic-wait task must be present")
	require.NotNil(t, gw.Wait)
	require.Len(t, gw.Wait.Terms, 1)
	require.Equal(t, "gen1", gw.Wait.Terms[0].Label, "empty label should fall back to name")
	require.NotNil(t, gw.Wait.Terms[0].ExprCEL, "cel_expr should be present")
	require.Equal(t, "x > 0", *gw.Wait.Terms[0].ExprCEL)
	require.Nil(t, gw.Wait.Terms[0].SignalKey, "generic term has no signal_key")
	require.Nil(t, gw.Wait.Terms[0].TimerName, "generic term has no timer_name")
	require.Equal(t, "wait", gw.WaitReason)

	// Verify the wait JSON round-trips through JSON correctly (arrays not null).
	wireBytes, err := json.Marshal(resp)
	require.NoError(t, err)
	require.Contains(t, string(wireBytes), `"inputs":{"deps":[],"signals":[],"timers":[]}`)
	require.Contains(t, string(wireBytes), `"terms":[`)
}

// TestAPIWorkflowGetEndpoint_NoWaitMetadata verifies that tasks without
// river:workflow_wait metadata emit no "wait" field (omitempty).
func TestAPIWorkflowGetEndpoint_NoWaitMetadata(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	endpoint, bundle := setupEndpoint(ctx, t, newWorkflowGetEndpoint)

	workflowID := "wf-no-wait"
	_ = insertWorkflowJobForTest(ctx, t, bundle, workflowID, "no-wait-test", "plain", nil, rivertype.JobStateCompleted)

	resp, err := endpoint.Execute(ctx, &workflowGetRequest{ID: workflowID})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 1)
	require.Nil(t, resp.Tasks[0].Wait, "task without wait metadata must have nil Wait")

	wireBytes, err := json.Marshal(resp)
	require.NoError(t, err)
	require.NotContains(t, string(wireBytes), `"wait":`, "wait must be omitted from JSON when nil")
}

//
// workflowTaskSignalsEndpoint tests
//

func emitSignalForTest(ctx context.Context, t *testing.T, bundle *setupEndpointTestBundle, workflowID, key string, payload []byte) *rivertype.WorkflowSignal {
	t.Helper()
	sig, err := bundle.exec.WorkflowSignalEmit(ctx, &riverdriver.WorkflowSignalEmitParams{
		WorkflowID: workflowID,
		SignalKey:  key,
		Payload:    payload,
		Now:        time.Now().UTC(),
	})
	require.NoError(t, err)
	return sig
}

func TestAPIWorkflowTaskSignalsEndpoint(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	endpoint, bundle := setupEndpoint(ctx, t, newWorkflowTaskSignalsEndpoint)

	workflowID := "wf-signals-test"

	// Emit three signals for the same key so we can test ordering and pagination.
	sig1 := emitSignalForTest(ctx, t, bundle, workflowID, "order.ready", []byte(`{"seq":1}`))
	sig2 := emitSignalForTest(ctx, t, bundle, workflowID, "order.ready", []byte(`{"seq":2}`))
	sig3 := emitSignalForTest(ctx, t, bundle, workflowID, "order.ready", []byte(`{"seq":3}`))

	// Fetch all three (desc=true → newest first).
	resp, err := endpoint.Execute(ctx, &workflowTaskSignalsRequest{
		ID:       workflowID,
		TaskName: "step1",
		Desc:     true,
		Limit:    20,
		Scope:    "history",
	})
	require.NoError(t, err)
	require.Len(t, resp.Signals, 3)
	require.False(t, resp.HasMore)
	require.Nil(t, resp.NextCursorID)
	require.Equal(t, "history", resp.Scope)

	// Newest-first order: sig3 > sig2 > sig1.
	require.Equal(t, int64(sig3.ID), int64(resp.Signals[0].ID))
	require.Equal(t, int64(sig2.ID), int64(resp.Signals[1].ID))
	require.Equal(t, int64(sig1.ID), int64(resp.Signals[2].ID))

	// Spot-check one signal's fields.
	require.Equal(t, "order.ready", resp.Signals[0].Key)
	require.Equal(t, 0, resp.Signals[0].Attempt)
	require.JSONEq(t, `{"seq":3}`, string(resp.Signals[0].Payload))
	require.Nil(t, resp.Signals[0].Source)
}

func TestAPIWorkflowTaskSignalsEndpoint_Pagination(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	endpoint, bundle := setupEndpoint(ctx, t, newWorkflowTaskSignalsEndpoint)

	workflowID := "wf-signals-paged"
	emitSignalForTest(ctx, t, bundle, workflowID, "tick", []byte(`{}`))
	emitSignalForTest(ctx, t, bundle, workflowID, "tick", []byte(`{}`))
	emitSignalForTest(ctx, t, bundle, workflowID, "tick", []byte(`{}`))

	// Fetch with limit=2 → has_more=true, next_cursor_id set.
	resp, err := endpoint.Execute(ctx, &workflowTaskSignalsRequest{
		ID:       workflowID,
		TaskName: "any",
		Desc:     true,
		Limit:    2,
		Scope:    "history",
	})
	require.NoError(t, err)
	require.Len(t, resp.Signals, 2)
	require.True(t, resp.HasMore, "has_more must be true when more rows exist")
	require.NotNil(t, resp.NextCursorID, "next_cursor_id must be set when has_more=true")

	// Fetch with limit=3 → all returned, no more.
	resp2, err := endpoint.Execute(ctx, &workflowTaskSignalsRequest{
		ID:       workflowID,
		TaskName: "any",
		Desc:     true,
		Limit:    3,
		Scope:    "history",
	})
	require.NoError(t, err)
	require.Len(t, resp2.Signals, 3)
	require.False(t, resp2.HasMore)
	require.Nil(t, resp2.NextCursorID)
}

func TestAPIWorkflowTaskSignalsEndpoint_KeyFilter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	endpoint, bundle := setupEndpoint(ctx, t, newWorkflowTaskSignalsEndpoint)

	workflowID := "wf-signals-keyed"
	emitSignalForTest(ctx, t, bundle, workflowID, "alpha", []byte(`{}`))
	emitSignalForTest(ctx, t, bundle, workflowID, "beta", []byte(`{}`))
	emitSignalForTest(ctx, t, bundle, workflowID, "alpha", []byte(`{}`))

	key := "alpha"
	resp, err := endpoint.Execute(ctx, &workflowTaskSignalsRequest{
		ID:       workflowID,
		TaskName: "any",
		Key:      &key,
		Desc:     true,
		Limit:    20,
		Scope:    "history",
	})
	require.NoError(t, err)
	require.Len(t, resp.Signals, 2, "only alpha signals should be returned")
	for _, s := range resp.Signals {
		require.Equal(t, "alpha", s.Key)
	}
}

//
// workflowTaskWaitDiagnosticsEndpoint tests
//

func TestAPIWorkflowTaskWaitDiagnosticsEndpoint_Pending(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	endpoint, bundle := setupEndpoint(ctx, t, newWorkflowTaskWaitDiagnosticsEndpoint)

	workflowID := "wf-diag-pending"
	// Insert a pending task that carries a signal-gated wait spec but no started_at.
	signalSpec := `{"expr":"sig1","terms":[{"name":"sig1","kind":"signal","key":"order.ready","label":"Order Ready"}]}`
	_ = insertWorkflowWaitJobForTest(ctx, t, bundle, workflowID, "diag-test", "step1",
		nil, rivertype.JobStatePending, signalSpec, "", "")

	resp, err := endpoint.Execute(ctx, &workflowTaskWaitDiagnosticsRequest{
		ID:       workflowID,
		TaskName: "step1",
	})
	require.NoError(t, err)

	// Phase: no signals emitted yet → expr not satisfied → "waiting" (WaitPhasePending).
	require.Equal(t, "waiting", resp.Phase)
	require.NotNil(t, resp.ExprResult)
	require.False(t, *resp.ExprResult)

	// Terms: one term (sig1), not satisfied.
	require.Len(t, resp.Terms, 1)
	require.Equal(t, "sig1", resp.Terms[0].Name)
	require.False(t, resp.Terms[0].Satisfied)

	// Inputs must be non-nil empty slices.
	require.NotNil(t, resp.Inputs.Deps)
	require.NotNil(t, resp.Inputs.Signals)
	require.NotNil(t, resp.Inputs.Timers)

	// inspected_at should be set (close to now).
	require.WithinDuration(t, time.Now(), resp.InspectedAt, 5*time.Second)

	// signal_scan_limit must be populated.
	require.Equal(t, waitDiagDefaultScanLimit, resp.SignalScanLimit)

	// Verify JSON shape — inputs arrays must be [] not null.
	wireBytes, err := json.Marshal(resp)
	require.NoError(t, err)
	require.Contains(t, string(wireBytes), `"inputs":{"deps":[],"signals":[],"timers":[]}`)
}

func TestAPIWorkflowTaskWaitDiagnosticsEndpoint_NoWait(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	endpoint, bundle := setupEndpoint(ctx, t, newWorkflowTaskWaitDiagnosticsEndpoint)

	workflowID := "wf-diag-nowait"
	// Task without any wait spec → phase "not_started".
	_ = insertWorkflowJobForTest(ctx, t, bundle, workflowID, "diag-nowait-test", "plain", nil, rivertype.JobStatePending)

	resp, err := endpoint.Execute(ctx, &workflowTaskWaitDiagnosticsRequest{
		ID:       workflowID,
		TaskName: "plain",
	})
	require.NoError(t, err)
	require.Equal(t, "not_started", resp.Phase)
	require.Len(t, resp.Terms, 0)
}
