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

// TestAPIWorkflowGetEndpoint_WaitReason verifies that pending tasks report
// "dependencies" so the frontend can render "Blocked by dependencies"
// instead of falling through to "Not waiting" for OSS workflows.
func TestAPIWorkflowGetEndpoint_WaitReason(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	endpoint, bundle := setupEndpoint(ctx, t, newWorkflowGetEndpoint)

	workflowID := "wf-wait-reason"
	_ = insertWorkflowJobForTest(ctx, t, bundle, workflowID, "wait-test", "a", nil, rivertype.JobStateCompleted)
	_ = insertWorkflowJobForTest(ctx, t, bundle, workflowID, "wait-test", "b", []string{"a"}, rivertype.JobStatePending)
	_ = insertWorkflowJobForTest(ctx, t, bundle, workflowID, "wait-test", "c", nil, rivertype.JobStateRunning)
	_ = insertWorkflowJobForTest(ctx, t, bundle, workflowID, "wait-test", "d", nil, rivertype.JobStateCancelled)

	resp, err := endpoint.Execute(ctx, &workflowGetRequest{ID: workflowID})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 4)

	byName := map[string]string{}
	for _, task := range resp.Tasks {
		byName[task.Name] = task.WaitReason
	}
	require.Equal(t, "none", byName["a"], "completed task should not be waiting")
	require.Equal(t, "dependencies", byName["b"], "pending task should be blocked by dependencies")
	require.Equal(t, "none", byName["c"], "running task should not be waiting")
	require.Equal(t, "none", byName["d"], "cancelled task should not be waiting")
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
