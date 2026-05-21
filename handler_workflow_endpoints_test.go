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
