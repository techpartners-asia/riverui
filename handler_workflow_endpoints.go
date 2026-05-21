package riverui

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"time"

	"github.com/riverqueue/apiframe/apiendpoint"
	"github.com/riverqueue/apiframe/apierror"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
	"github.com/riverqueue/river/rivertype"

	"riverqueue.com/riverui/internal/apibundle"
)

// OSS workflow metadata keys (mirrors github.com/riverqueue/river/internal/rivercommon).
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
// the React frontend renders OSS workflows without modification. Endpoints
// are mounted under the /api/pro/workflows prefix to match the frontend.
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
		Pattern:    "GET /api/pro/workflows/{id}",
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
		Schema:     a.Client.Schema(),
		WorkflowID: req.ID,
	})
	if err != nil {
		return nil, fmt.Errorf("error fetching workflow tasks: %w", err)
	}
	if len(rows) == 0 {
		return nil, apierror.NewNotFoundf("Workflow not found: %s.", req.ID)
	}

	slices.SortFunc(rows, func(a, b *rivertype.JobRow) int {
		return cmp.Compare(a.ID, b.ID)
	})

	tasks := make([]*workflowTaskSerializable, 0, len(rows))
	var workflowName string
	for _, row := range rows {
		task, name, err := buildWorkflowTask(row, req.ID)
		if err != nil {
			a.Logger.WarnContext(ctx, "skipping workflow task with unparseable metadata",
				slog.Int64("job_id", row.ID),
				slog.String("error", err.Error()))
			continue
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
		Pattern:    "POST /api/pro/workflows/{id}/cancel",
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
	CancelledJobs []*RiverJobMinimal `json:"cancelled_jobs"`
}

func (a *workflowCancelEndpoint[TTx]) Execute(ctx context.Context, req *workflowCancelRequest) (*workflowCancelResponse, error) {
	now := time.Now()
	rows, err := a.DB.JobCancelWorkflow(ctx, &riverdriver.JobCancelWorkflowParams{
		CancelAttemptedAt: now,
		ControlTopic:      "river_control",
		Now:               now,
		Reason:            "cancelled by riverui",
		Schema:            a.Client.Schema(),
		WorkflowID:        req.ID,
	})
	if err != nil {
		return nil, fmt.Errorf("error cancelling workflow: %w", err)
	}
	slices.SortFunc(rows, func(a, b *rivertype.JobRow) int {
		return cmp.Compare(a.ID, b.ID)
	})
	return &workflowCancelResponse{
		CancelledJobs: sliceutil.Map(rows, riverJobToSerializableJobMinimal),
	}, nil
}

//
// workflowListEndpoint — aggregates workflow rows by workflow_id.
// Reads job pages in batches and groups them in memory; suitable for
// dashboards with up to a few thousand workflow tasks in flight.
//

type workflowListEndpoint[TTx any] struct {
	apibundle.APIBundle[TTx]
	apiendpoint.Endpoint[workflowListRequest, workflowListResponse]
}

func newWorkflowListEndpoint[TTx any](bundle apibundle.APIBundle[TTx]) *workflowListEndpoint[TTx] {
	return &workflowListEndpoint[TTx]{APIBundle: bundle}
}

func (*workflowListEndpoint[TTx]) Meta() *apiendpoint.EndpointMeta {
	return &apiendpoint.EndpointMeta{
		Pattern:    "GET /api/pro/workflows",
		StatusCode: http.StatusOK,
	}
}

type workflowListRequest struct {
	Limit *int   `json:"-" validate:"omitempty,min=1,max=1000"`
	State string `json:"-" validate:"omitempty,oneof=active inactive"`
}

func (req *workflowListRequest) ExtractRaw(r *http.Request) error {
	if v := r.URL.Query().Get("limit"); v != "" {
		var n int
		_, err := fmt.Sscanf(v, "%d", &n)
		if err == nil {
			req.Limit = &n
		}
	}
	req.State = r.URL.Query().Get("state")
	return nil
}

type workflowListItem struct {
	CountAvailable int       `json:"count_available"`
	CountCancelled int       `json:"count_cancelled"`
	CountCompleted int       `json:"count_completed"`
	CountDiscarded int       `json:"count_discarded"`
	CountFailedDeps int      `json:"count_failed_deps"`
	CountPending   int       `json:"count_pending"`
	CountRetryable int       `json:"count_retryable"`
	CountRunning   int       `json:"count_running"`
	CountScheduled int       `json:"count_scheduled"`
	CreatedAt      time.Time `json:"created_at"`
	ID             string    `json:"id"`
	Name           *string   `json:"name"`
}

type workflowListResponse struct {
	Data []*workflowListItem `json:"data"`
}

func (a *workflowListEndpoint[TTx]) Execute(ctx context.Context, req *workflowListRequest) (*workflowListResponse, error) {
	limit := 100
	if req.Limit != nil {
		limit = *req.Limit
	}

	// Walk river_job by id DESC so the newest (and most likely active) workflow
	// tasks land in the first batch. State filtering happens post-aggregation
	// because a workflow's "active" status is derived from its mix of task
	// states. The hard scan cap keeps memory bounded on huge tables; the loop
	// also stops early once enough buckets satisfy the filter for the limit.
	const scanBatch = 1000
	const maxScan = 50000

	var (
		beforeID  int64 = 0
		first           = true
		buckets         = map[string]*workflowListItem{}
		taskCount       = 0
		exhausted       = false
	)
	for taskCount < maxScan && !exhausted {
		whereClause := `metadata ? 'river:workflow_id'`
		if !first {
			whereClause += ` AND id < ` + strconv.FormatInt(beforeID, 10)
		}
		first = false
		rows, err := a.DB.JobList(ctx, &riverdriver.JobListParams{
			Max:           scanBatch,
			OrderByClause: `id DESC`,
			Schema:        a.Client.Schema(),
			WhereClause:   whereClause,
		})
		if err != nil {
			return nil, fmt.Errorf("error listing workflow tasks: %w", err)
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			taskCount++
			beforeID = row.ID
			if err := mergeIntoWorkflowList(buckets, row); err != nil {
				a.Logger.WarnContext(ctx, "skipping job with unparseable workflow metadata",
					slog.Int64("job_id", row.ID),
					slog.String("error", err.Error()))
				continue
			}
		}
		// Early-exit heuristic: once we have at least 4x the requested limit
		// in matching buckets we have a good chance of filling `limit` after
		// state filtering even if some workflows don't match.
		matching := 0
		for _, v := range buckets {
			if workflowStateMatches(v, req.State) {
				matching++
			}
		}
		if matching >= limit*4 {
			break
		}
		if len(rows) < scanBatch {
			exhausted = true
		}
	}

	items := make([]*workflowListItem, 0, len(buckets))
	for _, v := range buckets {
		if !workflowStateMatches(v, req.State) {
			continue
		}
		items = append(items, v)
	}
	// Sort by CreatedAt desc, then by ID asc as tiebreaker.
	slices.SortFunc(items, func(a, b *workflowListItem) int {
		if c := b.CreatedAt.Compare(a.CreatedAt); c != 0 {
			return c
		}
		return cmp.Compare(a.ID, b.ID)
	})
	if len(items) > limit {
		items = items[:limit]
	}
	return &workflowListResponse{Data: items}, nil
}

func mergeIntoWorkflowList(buckets map[string]*workflowListItem, row *rivertype.JobRow) error {
	var meta map[string]json.RawMessage
	if err := json.Unmarshal(row.Metadata, &meta); err != nil {
		return fmt.Errorf("parse metadata for job %d: %w", row.ID, err)
	}
	var id string
	if raw, ok := meta[metadataKeyWorkflowID]; ok {
		_ = json.Unmarshal(raw, &id)
	}
	if id == "" {
		return nil
	}
	item, ok := buckets[id]
	if !ok {
		item = &workflowListItem{ID: id, CreatedAt: row.CreatedAt}
		buckets[id] = item
	}
	if row.CreatedAt.Before(item.CreatedAt) {
		item.CreatedAt = row.CreatedAt
	}
	if item.Name == nil {
		var name string
		if raw, ok := meta[metadataKeyWorkflowName]; ok {
			_ = json.Unmarshal(raw, &name)
		}
		if name != "" {
			item.Name = &name
		}
	}
	switch row.State {
	case rivertype.JobStateAvailable:
		item.CountAvailable++
	case rivertype.JobStateCancelled:
		item.CountCancelled++
	case rivertype.JobStateCompleted:
		item.CountCompleted++
	case rivertype.JobStateDiscarded:
		item.CountDiscarded++
	case rivertype.JobStatePending:
		item.CountPending++
	case rivertype.JobStateRetryable:
		item.CountRetryable++
	case rivertype.JobStateRunning:
		item.CountRunning++
	case rivertype.JobStateScheduled:
		item.CountScheduled++
	}
	return nil
}

func workflowStateMatches(w *workflowListItem, requested string) bool {
	if requested == "" {
		return true
	}
	hasActive := w.CountAvailable+w.CountPending+w.CountRetryable+w.CountRunning+w.CountScheduled > 0
	switch requested {
	case "active":
		return hasActive
	case "inactive":
		return !hasActive
	}
	return true
}

func intLit(n int64) string {
	return fmt.Sprintf("%d", n)
}

//
// workflowRetryEndpoint
//

type workflowRetryEndpoint[TTx any] struct {
	apibundle.APIBundle[TTx]
	apiendpoint.Endpoint[workflowRetryRequest, workflowRetryResponse]
}

func newWorkflowRetryEndpoint[TTx any](bundle apibundle.APIBundle[TTx]) *workflowRetryEndpoint[TTx] {
	return &workflowRetryEndpoint[TTx]{APIBundle: bundle}
}

func (*workflowRetryEndpoint[TTx]) Meta() *apiendpoint.EndpointMeta {
	return &apiendpoint.EndpointMeta{
		Pattern:    "POST /api/pro/workflows/{id}/retry",
		StatusCode: http.StatusOK,
	}
}

type workflowRetryRequest struct {
	ID           string `json:"-"`
	Mode         string `json:"mode" validate:"omitempty,oneof=all failed_and_downstream failed_only"`
	ResetHistory bool   `json:"reset_history"`
}

func (req *workflowRetryRequest) ExtractRaw(r *http.Request) error {
	req.ID = r.PathValue("id")
	if r.ContentLength > 0 && r.Header.Get("Content-Type") == "application/json" {
		if err := json.NewDecoder(r.Body).Decode(req); err != nil {
			return apierror.NewBadRequestf("invalid retry body: %s", err)
		}
	}
	if req.Mode == "" {
		req.Mode = "failed_and_downstream"
	}
	return nil
}

type workflowRetryResponse struct {
	RetriedJobs []*RiverJobMinimal `json:"retried_jobs"`
}

func (a *workflowRetryEndpoint[TTx]) Execute(ctx context.Context, req *workflowRetryRequest) (*workflowRetryResponse, error) {
	rows, err := a.DB.JobRetryWorkflow(ctx, &riverdriver.JobRetryWorkflowParams{
		Mode:         req.Mode,
		Now:          time.Now(),
		ResetHistory: req.ResetHistory,
		Schema:       a.Client.Schema(),
		WorkflowID:   req.ID,
	})
	if err != nil {
		return nil, fmt.Errorf("error retrying workflow: %w", err)
	}
	return &workflowRetryResponse{
		RetriedJobs: sliceutil.Map(rows, riverJobToSerializableJobMinimal),
	}, nil
}

// buildWorkflowTask unpacks a JobRow's workflow metadata into the response
// task shape.
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
	deps := []string{}
	if raw, ok := meta[metadataKeyWorkflowDeps]; ok {
		_ = json.Unmarshal(raw, &deps)
	}
	if deps == nil {
		deps = []string{}
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
