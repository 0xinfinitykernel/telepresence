package tracing

import (
	"context"
	"fmt"
	"sync"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// An otlpShim is a pretend client that just collects spans without exporting them
type otlpShim struct {
	spans []*tracepb.ResourceSpans
	mu    sync.RWMutex
}

func (ts *otlpShim) Start(ctx context.Context) error {
	return nil
}
func (ts *otlpShim) Stop(ctx context.Context) error {
	return nil
}
func (ts *otlpShim) UploadTraces(ctx context.Context, protoSpans []*tracepb.ResourceSpans) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.spans = append(ts.spans, protoSpans...)
	return nil
}

func (ts *otlpShim) dumpTraces(ctx context.Context) ([]byte, error) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	message := &tracepb.TracesData{ResourceSpans: ts.spans}
	ret, err := proto.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize spans: %w", err)
	}
	return ret, nil
}
