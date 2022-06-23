package commands

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/datawire/dlib/dlog"
	"github.com/spf13/cobra"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
)

func PushTraces() *cobra.Command {
	return &cobra.Command{
		Use:  "upload-traces",
		Args: cobra.ExactArgs(2),

		Short: "Upload Traces",
		Long:  "Upload Traces to a Jaeger instance",
		RunE:  pushTraces,
	}
}

func traceClient(url string) (otlptrace.Client, error) {
	client := otlptracegrpc.NewClient(
		otlptracegrpc.WithEndpoint(url),
		otlptracegrpc.WithInsecure(),
	)
	return client, nil
}

func pushTraces(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
	defer cancel()
	zipFile, jaegerTarget := args[0], args[1]
	f, err := os.Open(zipFile)
	if err != nil {
		return fmt.Errorf("failed to open %s: %w", zipFile, err)
	}
	defer f.Close()
	zipR, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("failed to unzip %s: %w", zipFile, err)
	}
	defer zipR.Close()
	client, err := traceClient(jaegerTarget)
	if err != nil {
		return err
	}
	data, err := io.ReadAll(zipR)
	if err != nil {
		return err
	}
	msg := &tracepb.TracesData{}
	err = proto.Unmarshal(data, msg)
	if err != nil {
		return err
	}
	dlog.Debugf(ctx, "Starting upload of %d traces", len(msg.ResourceSpans))
	err = client.Start(ctx)
	if err != nil {
		return err
	}
	err = client.UploadTraces(ctx, msg.ResourceSpans)
	if err != nil {
		return err
	}
	err = client.Stop(ctx)
	if err != nil {
		return err
	}
	return nil
}
