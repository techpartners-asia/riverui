package riverui

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/binary"
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
	WaitReason          string   `json:"wait_reason"`
	WorkflowID          string   `json:"workflow_id"`
}

// computeWaitReason classifies why a workflow task is currently not running.
// OSS workflows have no wait-on-signal feature (pro-only), so the only
// blocking condition is unfinished dependencies: a task sits in `pending`
// until workflowscheduler promotes it, which happens once every dep reaches
// a terminal state. Anything not pending is, by definition, not waiting.
func computeWaitReason(state rivertype.JobState) string {
	if state == rivertype.JobStatePending {
		return "dependencies"
	}
	return "none"
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
	CountAvailable  int       `json:"count_available"`
	CountCancelled  int       `json:"count_cancelled"`
	CountCompleted  int       `json:"count_completed"`
	CountDiscarded  int       `json:"count_discarded"`
	CountFailedDeps int       `json:"count_failed_deps"`
	CountPending    int       `json:"count_pending"`
	CountRetryable  int       `json:"count_retryable"`
	CountRunning    int       `json:"count_running"`
	CountScheduled  int       `json:"count_scheduled"`
	CreatedAt       time.Time `json:"created_at"`
	ID              string    `json:"id"`
	Name            *string   `json:"name"`

	// Internal staging used to compute CountFailedDeps after all rows for a
	// workflow have been scanned. Tasks are kept here as raw rows so we can
	// distinguish user-cancelled tasks (no failed dep) from cascade-cancelled
	// tasks (at least one dep is cancelled or discarded, no ignore flag set).
	// These fields are not serialized to the client.
	taskStates    map[string]rivertype.JobState `json:"-"`
	cancelledRows []cancelledTaskInfo           `json:"-"`
}

// cancelledTaskInfo carries the metadata needed to reclassify a cancelled
// task as cascade-failed after the full task set for its workflow has been
// scanned.
type cancelledTaskInfo struct {
	deps                []string
	ignoreCancelledDeps bool
	ignoreDiscardedDeps bool
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

	finalizeCascadeFailures(buckets)

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
		item = &workflowListItem{
			ID:         id,
			CreatedAt:  row.CreatedAt,
			taskStates: map[string]rivertype.JobState{},
		}
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

	var taskName string
	if raw, ok := meta[metadataKeyWorkflowTask]; ok {
		_ = json.Unmarshal(raw, &taskName)
	}
	if taskName != "" {
		item.taskStates[taskName] = row.State
	}

	switch row.State {
	case rivertype.JobStateAvailable:
		item.CountAvailable++
	case rivertype.JobStateCancelled:
		// Stash deps + ignore flags so finalizeCascadeFailures can reclassify
		// this task as failed-deps once every sibling's state is known.
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
		item.cancelledRows = append(item.cancelledRows, cancelledTaskInfo{
			deps:                deps,
			ignoreCancelledDeps: ignoreBool(metadataKeyWorkflowIgnoreCancelledDeps),
			ignoreDiscardedDeps: ignoreBool(metadataKeyWorkflowIgnoreDiscardedDeps),
		})
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

// finalizeCascadeFailures walks each bucket's cancelled tasks and moves any
// that were cancelled because of a failed dependency (cancelled or discarded
// upstream task, ignore flag not set) from CountCancelled into the dedicated
// CountFailedDeps bucket. This must run after the scan loop completes so the
// per-task state map is fully populated. Without this, the workflow list page
// can't distinguish cascade failures from user-initiated cancellations.
func finalizeCascadeFailures(buckets map[string]*workflowListItem) {
	for _, item := range buckets {
		for _, c := range item.cancelledRows {
			if !cancelledFromFailedDep(c, item.taskStates) {
				continue
			}
			item.CountCancelled--
			item.CountFailedDeps++
		}
		// Free the staging slices/maps before the response is serialized.
		item.cancelledRows = nil
		item.taskStates = nil
	}
}

// cancelledFromFailedDep returns true if at least one of c.deps is in a
// failed terminal state (cancelled or discarded) without the corresponding
// ignore flag being set on c.
func cancelledFromFailedDep(c cancelledTaskInfo, states map[string]rivertype.JobState) bool {
	for _, dep := range c.deps {
		state, known := states[dep]
		if !known {
			continue
		}
		if state == rivertype.JobStateCancelled && !c.ignoreCancelledDeps {
			return true
		}
		if state == rivertype.JobStateDiscarded && !c.ignoreDiscardedDeps {
			return true
		}
	}
	return false
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
		WaitReason:          computeWaitReason(row.State),
		WorkflowID:          workflowID,
	}, workflowName, nil
}

//
// workflowRerunEndpoint
//

type workflowRerunEndpoint[TTx any] struct {
	apibundle.APIBundle[TTx]
	apiendpoint.Endpoint[workflowRerunRequest, workflowRerunResponse]
}

func newWorkflowRerunEndpoint[TTx any](bundle apibundle.APIBundle[TTx]) *workflowRerunEndpoint[TTx] {
	return &workflowRerunEndpoint[TTx]{APIBundle: bundle}
}

func (*workflowRerunEndpoint[TTx]) Meta() *apiendpoint.EndpointMeta {
	return &apiendpoint.EndpointMeta{
		Pattern:    "POST /api/pro/workflows/{id}/rerun",
		StatusCode: http.StatusOK,
	}
}

type workflowRerunRequest struct {
	ID string `json:"-" validate:"required"`
}

func (req *workflowRerunRequest) ExtractRaw(r *http.Request) error {
	req.ID = r.PathValue("id")
	return nil
}

type workflowRerunResponse struct {
	WorkflowID  string              `json:"workflow_id"`
	InsertedJobs []*RiverJobMinimal `json:"inserted_jobs"`
}

// Execute reads the original workflow's task definitions and inserts a fresh
// copy under a new workflow ID. The new tasks are inserted in their initial
// non-terminal state (pending if they have deps, available otherwise);
// scheduling, retries, and cascade behaviour follow the normal workflow path.
//
// The original workflow is not modified; this is a true re-run from scratch,
// not a retry. Useful for re-running a successfully completed pipeline (e.g.
// "re-run yesterday's billing for a corrected input file") or starting over
// after a cancellation.
func (a *workflowRerunEndpoint[TTx]) Execute(ctx context.Context, req *workflowRerunRequest) (*workflowRerunResponse, error) {
	origRows, err := a.DB.JobGetWorkflowTasks(ctx, &riverdriver.JobGetWorkflowTasksParams{
		Schema:     a.Client.Schema(),
		WorkflowID: req.ID,
	})
	if err != nil {
		return nil, fmt.Errorf("error loading original workflow tasks: %w", err)
	}
	if len(origRows) == 0 {
		return nil, apierror.NewNotFoundf("Workflow not found")
	}

	newWorkflowID := newWorkflowULID()
	now := time.Now()

	jobs := make([]*riverdriver.JobInsertFullParams, 0, len(origRows))
	for _, row := range origRows {
		params, err := buildRerunInsertParams(row, newWorkflowID, a.Client.Schema(), now)
		if err != nil {
			return nil, fmt.Errorf("error building rerun for task id=%d: %w", row.ID, err)
		}
		jobs = append(jobs, params)
	}

	inserted, err := a.DB.JobInsertFullMany(ctx, &riverdriver.JobInsertFullManyParams{
		Jobs:   jobs,
		Schema: a.Client.Schema(),
	})
	if err != nil {
		return nil, fmt.Errorf("error inserting rerun workflow tasks: %w", err)
	}

	a.Logger.InfoContext(ctx, "rerun workflow",
		slog.String("original_workflow_id", req.ID),
		slog.String("new_workflow_id", newWorkflowID),
		slog.Int("tasks", len(inserted)))

	return &workflowRerunResponse{
		WorkflowID:   newWorkflowID,
		InsertedJobs: sliceutil.Map(inserted, riverJobToSerializableJobMinimal),
	}, nil
}

// buildRerunInsertParams takes an original workflow task row and returns a
// fresh JobInsertFullParams that, when inserted, becomes a new task in a
// new workflow with the same args, queue, kind, and dep wiring.
func buildRerunInsertParams(row *rivertype.JobRow, newWorkflowID, schema string, now time.Time) (*riverdriver.JobInsertFullParams, error) {
	var origMeta map[string]json.RawMessage
	if err := json.Unmarshal(row.Metadata, &origMeta); err != nil {
		return nil, fmt.Errorf("parse metadata: %w", err)
	}

	// Carry over only the workflow-shape keys; drop anything else (output,
	// cancel_reason, attempts, etc.) so the new tasks start clean.
	newMeta := map[string]json.RawMessage{}
	carry := func(key string) {
		if v, ok := origMeta[key]; ok {
			newMeta[key] = v
		}
	}
	carry(metadataKeyWorkflowTask)
	carry(metadataKeyWorkflowName)
	carry(metadataKeyWorkflowDeps)
	carry(metadataKeyWorkflowIgnoreCancelledDeps)
	carry(metadataKeyWorkflowIgnoreDeletedDeps)
	carry(metadataKeyWorkflowIgnoreDiscardedDeps)

	wfIDBytes, _ := json.Marshal(newWorkflowID)
	newMeta[metadataKeyWorkflowID] = wfIDBytes

	metaBytes, err := json.Marshal(newMeta)
	if err != nil {
		return nil, fmt.Errorf("marshal metadata: %w", err)
	}

	// A task with deps starts pending and gets promoted by the scheduler
	// once its deps reach a terminal state; everything else is immediately
	// available.
	state := rivertype.JobStateAvailable
	if _, hasDeps := newMeta[metadataKeyWorkflowDeps]; hasDeps {
		state = rivertype.JobStatePending
	}

	scheduledAt := now
	return &riverdriver.JobInsertFullParams{
		EncodedArgs: row.EncodedArgs,
		Kind:        row.Kind,
		MaxAttempts: row.MaxAttempts,
		Metadata:    metaBytes,
		Priority:    row.Priority,
		Queue:       row.Queue,
		ScheduledAt: &scheduledAt,
		Schema:      schema,
		State:       state,
		Tags:        append([]string{}, row.Tags...),
	}, nil
}

// newWorkflowULID generates a ULID-shaped 26-character Crockford-base32 ID
// suitable as a workflow_id. This mirrors river's internal/workflowid
// generator without taking a dependency on an internal package.
func newWorkflowULID() string {
	const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	var raw [16]byte
	ms := uint64(time.Now().UnixMilli()) //nolint:gosec
	binary.BigEndian.PutUint64(raw[0:8], ms<<16)
	_, _ = rand.Read(raw[6:])

	// Encode 16 bytes (128 bits) into 26 Crockford base32 chars.
	out := make([]byte, 26)
	out[0] = crockford[(raw[0]&224)>>5]
	out[1] = crockford[raw[0]&31]
	out[2] = crockford[(raw[1]&248)>>3]
	out[3] = crockford[((raw[1]&7)<<2)|((raw[2]&192)>>6)]
	out[4] = crockford[(raw[2]&62)>>1]
	out[5] = crockford[((raw[2]&1)<<4)|((raw[3]&240)>>4)]
	out[6] = crockford[((raw[3]&15)<<1)|((raw[4]&128)>>7)]
	out[7] = crockford[(raw[4]&124)>>2]
	out[8] = crockford[((raw[4]&3)<<3)|((raw[5]&224)>>5)]
	out[9] = crockford[raw[5]&31]
	out[10] = crockford[(raw[6]&248)>>3]
	out[11] = crockford[((raw[6]&7)<<2)|((raw[7]&192)>>6)]
	out[12] = crockford[(raw[7]&62)>>1]
	out[13] = crockford[((raw[7]&1)<<4)|((raw[8]&240)>>4)]
	out[14] = crockford[((raw[8]&15)<<1)|((raw[9]&128)>>7)]
	out[15] = crockford[(raw[9]&124)>>2]
	out[16] = crockford[((raw[9]&3)<<3)|((raw[10]&224)>>5)]
	out[17] = crockford[raw[10]&31]
	out[18] = crockford[(raw[11]&248)>>3]
	out[19] = crockford[((raw[11]&7)<<2)|((raw[12]&192)>>6)]
	out[20] = crockford[(raw[12]&62)>>1]
	out[21] = crockford[((raw[12]&1)<<4)|((raw[13]&240)>>4)]
	out[22] = crockford[((raw[13]&15)<<1)|((raw[14]&128)>>7)]
	out[23] = crockford[(raw[14]&124)>>2]
	out[24] = crockford[((raw[14]&3)<<3)|((raw[15]&224)>>5)]
	out[25] = crockford[raw[15]&31]
	return string(out)
}
