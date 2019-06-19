package observability

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/go-gorp/gorp"
	"github.com/gorilla/mux"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"go.opencensus.io/trace"

	"github.com/ovh/cds/engine/api/cache"
	"github.com/ovh/cds/sdk/log"
	"github.com/ovh/cds/sdk/tracingutils"
)

// Tags contants
const (
	TagWorkflow           = "workflow"
	TagWorkflowRun        = "workflow_run"
	TagProjectKey         = "project_key"
	TagWorkflowNodeRun    = "workflow_node_run"
	TagWorkflowNodeJobRun = "workflow_node_job_run"
	TagJob                = "job"
	TagWorkflowNode       = "workflow_node"
	TagPipelineID         = "pipeline_id"
	TagPipelineDeep       = "pipeline_deep"
	TagWorker             = "worker"
	TagToken              = "token"
	TagPermission         = "permission"
)

// LinkTo a traceID
func LinkTo(ctx context.Context, traceID [16]byte) {
	s := Current(ctx)
	if s == nil {
		return
	}

	s.AddLink(
		trace.Link{
			TraceID: trace.TraceID(traceID),
		},
	)
}

// Current return the current span
func Current(ctx context.Context, tags ...trace.Attribute) *trace.Span {
	if ctx == nil {
		return nil
	}
	span := trace.FromContext(ctx)
	if span == nil {
		return nil
	}
	if len(tags) > 0 {
		span.AddAttributes(tags...)
	}
	return span
}

// Tag is helper function to instanciate trace.Attribute
func Tag(key string, value interface{}) trace.Attribute {
	return trace.StringAttribute(key, fmt.Sprintf("%v", value))
}

// Span start a new span from the parent context
func Span(ctx context.Context, name string, tags ...trace.Attribute) (context.Context, func()) {
	if ctx == nil {
		return context.Background(), func() {}
	}
	var span *trace.Span
	ctx, span = trace.StartSpan(ctx, name)
	if len(tags) > 0 {
		span.AddAttributes(tags...)
	}
	ctx = tracingutils.SpanContextToContext(ctx, span.SpanContext())
	return ctx, span.End
}

func findPrimaryKeyFromRequest(req *http.Request, db gorp.SqlExecutor, store cache.Store) (string, bool) {
	vars := mux.Vars(req)
	pkey := vars["key"]
	if pkey == "" {
		pkey = vars["permProjectKey"]
	}

	if pkey == "" {
		id, _ := strconv.ParseInt(vars["id"], 10, 64)
		//The ID found may be a node run job, let's try to find the project key behing
		if id <= 0 {
			id, _ = strconv.ParseInt(vars["permID"], 10, 64)
		}
		if id != 0 {
			var err error
			cacheKey := cache.Key("api:FindProjetKeyForNodeRunJob:", fmt.Sprintf("%v", id))
			if !store.Get(cacheKey, &pkey) {
				pkey, err = findProjetKeyForNodeRunJob(db, id)
				if err != nil {
					log.Error("tracingMiddleware> %v", err)
					return "", false
				}
				store.SetWithTTL(cacheKey, pkey, 60*15)
			}
		}
	}

	return pkey, pkey != ""
}

// NewViewLast creates a new view via aggregation LastValue()
func NewViewLast(name string, s *stats.Int64Measure, tags []tag.Key) *view.View {
	return &view.View{
		Name:        name,
		Description: s.Description(),
		Measure:     s,
		Aggregation: view.LastValue(),
		TagKeys:     tags,
	}
}

// NewViewLastFloat64 creates a new view via aggregation LastValue()
func NewViewLastFloat64(name string, s *stats.Float64Measure, tags []tag.Key) *view.View {
	return &view.View{
		Name:        name,
		Description: s.Description(),
		Measure:     s,
		Aggregation: view.LastValue(),
		TagKeys:     tags,
	}
}

// NewViewCount creates a new view via aggregation Count()
func NewViewCount(name string, s *stats.Int64Measure, tags []tag.Key) *view.View {
	return &view.View{
		Name:        name,
		Description: s.Description(),
		Measure:     s,
		Aggregation: view.Count(),
		TagKeys:     tags,
	}
}
