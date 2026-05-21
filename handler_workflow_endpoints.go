package riverui

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"time"

	"github.com/riverqueue/apiframe/apiendpoint"
	"github.com/riverqueue/apiframe/apierror"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
	"github.com/riverqueue/river/rivertype"

	"riverqueue.com/riverui/internal/apibundle"
)

// OSS workflow metadata keys (mirrors github.com/riverqueue/river/internal/rivercommon).
// Defined as string constants here so this package does not depend on the
// internal package.
const (
	metadataKeyWorkflowDeps                = "river:workflow_deps"
	metadataKeyWorkflowID                  = "river:workflow_id"
	metadataKeyWorkflowIgnoreCancelledDeps = "river:workflow_ignore_cancelled_deps"
	metadataKeyWorkflowIgnoreDeletedDeps   = "river:workflow_ignore_deleted_deps"
	metadataKeyWorkflowIgnoreDiscardedDeps = "river:workflow_ignore_discarded_deps"
	metadataKeyWorkflowName                = "river:workflow_name"
	metadataKeyWorkflowTask                = "river:workflow_task"
)

// workflowTaskSerializable is the response shape consumed by riverui's
// WorkflowDiagram component. Field names mirror riverproui's wire format so
// the React frontend renders OSS workflows without modification.
type workflowTaskSerializable struct {
	*RiverJob

	Deps                []string `json:"deps"`
	IgnoreCancelledDeps bool     `json:"ignore_cancelled_deps"`
	IgnoreDeletedDeps   bool     `json:"ignore_deleted_deps"`
	IgnoreDiscardedDeps bool     `json:"ignore_discarded_deps"`
	Name                string   `json:"name"`
	WorkflowID          string   `json:"workflow_id"`
}

//
// workflowGetEndpoint
//

type workflowGetEndpoint[TTx any] struct {
	apibundle.APIBundle[TTx]
	apiendpoint.Endpoint[workflowGetRequest, workflowGetResponse]
}

func newWorkflowGetEndpoint[TTx any](bundle apibundle.APIBundle[TTx]) *workflowGetEndpoint[TTx] {
	return &workflowGetEndpoint[TTx]{APIBundle: bundle}
}

func (*workflowGetEndpoint[TTx]) Meta() *apiendpoint.EndpointMeta {
	return &apiendpoint.EndpointMeta{
		Pattern:    "GET /api/workflows/{id}",
		StatusCode: http.StatusOK,
	}
}

type workflowGetRequest struct {
	ID string `json:"-" validate:"required"`
}

func (req *workflowGetRequest) ExtractRaw(r *http.Request) error {
	req.ID = r.PathValue("id")
	return nil
}

type workflowGetResponse struct {
	ID    string                      `json:"id"`
	Name  string                      `json:"name"`
	Tasks []*workflowTaskSerializable `json:"tasks"`
}

func (a *workflowGetEndpoint[TTx]) Execute(ctx context.Context, req *workflowGetRequest) (*workflowGetResponse, error) {
	rows, err := a.DB.JobGetWorkflowTasks(ctx, &riverdriver.JobGetWorkflowTasksParams{
		WorkflowID: req.ID,
	})
	if err != nil {
		return nil, fmt.Errorf("error fetching workflow tasks: %w", err)
	}
	if len(rows) == 0 {
		return nil, apierror.NewNotFoundf("Workflow not found: %s.", req.ID)
	}

	// Sort by ID for stable ordering.
	slices.SortFunc(rows, func(a, b *rivertype.JobRow) int {
		return int(a.ID - b.ID)
	})

	tasks := make([]*workflowTaskSerializable, 0, len(rows))
	var workflowName string
	for _, row := range rows {
		task, name, err := buildWorkflowTask(row, req.ID)
		if err != nil {
			return nil, err
		}
		if workflowName == "" && name != "" {
			workflowName = name
		}
		tasks = append(tasks, task)
	}

	return &workflowGetResponse{
		ID:    req.ID,
		Name:  workflowName,
		Tasks: tasks,
	}, nil
}

//
// workflowCancelEndpoint
//

type workflowCancelEndpoint[TTx any] struct {
	apibundle.APIBundle[TTx]
	apiendpoint.Endpoint[workflowCancelRequest, workflowCancelResponse]
}

func newWorkflowCancelEndpoint[TTx any](bundle apibundle.APIBundle[TTx]) *workflowCancelEndpoint[TTx] {
	return &workflowCancelEndpoint[TTx]{APIBundle: bundle}
}

func (*workflowCancelEndpoint[TTx]) Meta() *apiendpoint.EndpointMeta {
	return &apiendpoint.EndpointMeta{
		Pattern:    "POST /api/workflows/{id}/cancel",
		StatusCode: http.StatusOK,
	}
}

type workflowCancelRequest struct {
	ID string `json:"-" validate:"required"`
}

func (req *workflowCancelRequest) ExtractRaw(r *http.Request) error {
	req.ID = r.PathValue("id")
	return nil
}

type workflowCancelResponse struct {
	CancelledJobs []*RiverJob `json:"cancelled_jobs"`
}

func (a *workflowCancelEndpoint[TTx]) Execute(ctx context.Context, req *workflowCancelRequest) (*workflowCancelResponse, error) {
	now := time.Now()
	rows, err := a.DB.JobCancelWorkflow(ctx, &riverdriver.JobCancelWorkflowParams{
		CancelAttemptedAt: now,
		ControlTopic:      "river_control",
		Now:               now,
		Reason:            "cancelled by riverui",
		WorkflowID:        req.ID,
	})
	if err != nil {
		return nil, fmt.Errorf("error cancelling workflow: %w", err)
	}
	slices.SortFunc(rows, func(a, b *rivertype.JobRow) int {
		return int(a.ID - b.ID)
	})
	return &workflowCancelResponse{
		CancelledJobs: sliceutil.Map(rows, riverJobToSerializableJob),
	}, nil
}

// buildWorkflowTask unpacks a JobRow's workflow metadata into the response
// task shape. Returns the workflow name as a side effect (the same value is
// stored on every task, so callers usually only need to capture it once).
func buildWorkflowTask(row *rivertype.JobRow, workflowID string) (*workflowTaskSerializable, string, error) {
	var meta map[string]json.RawMessage
	if err := json.Unmarshal(row.Metadata, &meta); err != nil {
		return nil, "", fmt.Errorf("parse metadata for job %d: %w", row.ID, err)
	}

	var name string
	if raw, ok := meta[metadataKeyWorkflowTask]; ok {
		_ = json.Unmarshal(raw, &name)
	}
	var workflowName string
	if raw, ok := meta[metadataKeyWorkflowName]; ok {
		_ = json.Unmarshal(raw, &workflowName)
	}
	var deps []string
	if raw, ok := meta[metadataKeyWorkflowDeps]; ok {
		_ = json.Unmarshal(raw, &deps)
	}
	ignoreBool := func(key string) bool {
		raw, ok := meta[key]
		if !ok {
			return false
		}
		var b bool
		_ = json.Unmarshal(raw, &b)
		return b
	}

	return &workflowTaskSerializable{
		RiverJob:            riverJobToSerializableJob(row),
		Deps:                deps,
		IgnoreCancelledDeps: ignoreBool(metadataKeyWorkflowIgnoreCancelledDeps),
		IgnoreDeletedDeps:   ignoreBool(metadataKeyWorkflowIgnoreDeletedDeps),
		IgnoreDiscardedDeps: ignoreBool(metadataKeyWorkflowIgnoreDiscardedDeps),
		Name:                name,
		WorkflowID:          workflowID,
	}, workflowName, nil
}
