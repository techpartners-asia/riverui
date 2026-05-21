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
