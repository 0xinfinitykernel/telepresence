package commands

import (
	"compress/gzip"
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/datawire/dlib/dlog"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/telepresenceio/telepresence/rpc/v2/common"
	"github.com/telepresenceio/telepresence/v2/pkg/client/userd/trafficmgr"
	"github.com/telepresenceio/telepresence/v2/pkg/dnet"
	"github.com/telepresenceio/telepresence/v2/pkg/k8sapi"
	"github.com/telepresenceio/telepresence/v2/pkg/tracing"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	corev1 "k8s.io/api/core/v1"
	typedv1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

func TraceCommand() *cobra.Command {
	return &cobra.Command{
		Use:  "gather-traces",
		Args: cobra.NoArgs,

		Short: "Gather Traces",
		RunE:  gatherTraces,
		Annotations: map[string]string{
			CommandRequiresSession: "true",
		},
	}
}

func tracesFor(ctx context.Context, conn *grpc.ClientConn, acc *tracepb.TracesData) error {
	daemonCli := common.NewTracingServerClient(conn)
	result, err := daemonCli.DumpTraces(ctx, &emptypb.Empty{})
	if err != nil {
		return err
	}
	data := result.GetTraceData()
	msg := &tracepb.TracesData{}
	err = proto.Unmarshal(data, msg)
	if err != nil {
		return fmt.Errorf("failed to deserialize: %w", err)
	}
	acc.ResourceSpans = append(acc.ResourceSpans, msg.ResourceSpans...)
	return nil
}

func gatherTraces(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()
	memFS := afero.NewMemMapFs()
	err := memFS.Mkdir("traces", 0777)
	if err != nil {
		return fmt.Errorf("failed to mkdir: %w", err)
	}

	sess := trafficmgr.GetSession(ctx)
	traces := &tracepb.TracesData{}

	dConn, err := grpc.DialContext(ctx, fmt.Sprintf("localhost:%d", tracing.DaemonPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return err
	}
	defer dConn.Close()

	err = tracesFor(ctx, dConn, traces)
	if err != nil {
		return err
	}

	userdConn, err := grpc.DialContext(ctx, fmt.Sprintf("localhost:%d", tracing.UserdPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return err
	}
	defer userdConn.Close()

	err = tracesFor(ctx, userdConn, traces)
	if err != nil {
		return err
	}

	kpf, err := dnet.NewK8sPortForwardDialer(ctx, sess.GetRestConfig(), k8sapi.GetK8sInterface(ctx))
	grpcAddr := net.JoinHostPort(
		"svc/traffic-manager."+sess.GetManagerNamespace(),
		"15766")
	tc, tCancel := context.WithTimeout(ctx, 20*time.Second)
	defer tCancel()

	opts := []grpc.DialOption{grpc.WithContextDialer(kpf),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithNoProxy(),
		grpc.WithBlock(),
		grpc.WithReturnConnectionError(),
	}

	var conn *grpc.ClientConn
	if conn, err = grpc.DialContext(tc, grpcAddr, opts...); err != nil {
		return err
	}
	err = tracesFor(ctx, conn, traces)
	if err != nil {
		return err
	}

	lock := sync.Mutex{}

	err = sess.ForeachAgentPod(ctx, func(ctx context.Context, pi typedv1.PodInterface, pod *corev1.Pod) {
		if err != nil {
			dlog.Warnf(ctx, "unable to get traces for %s.%s: %v", pod.Name, pod.Namespace, err)
			return
		}
		name := fmt.Sprintf("%s.%s", pod.Name, pod.Namespace)
		// TODO: CONFIG!!!
		addr := net.JoinHostPort(name, "15766")
		tc, tCancel := context.WithTimeout(ctx, 20*time.Second)
		defer tCancel()

		opts := []grpc.DialOption{grpc.WithContextDialer(kpf),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithNoProxy(),
			grpc.WithBlock(),
			grpc.WithReturnConnectionError(),
		}

		var conn *grpc.ClientConn
		if conn, err = grpc.DialContext(tc, addr, opts...); err != nil {
			dlog.Warnf(ctx, "Unable to dial %s: %v", name, err)
			return
		}
		defer conn.Close()
		lock.Lock()
		defer lock.Unlock()
		err := tracesFor(tc, conn, traces)
		if err != nil {
			dlog.Warnf(ctx, "Unable to get traces for %s: %v", name, err)
			return
		}
	}, nil)
	if err != nil {
		return err
	}

	file, err := os.Create("./traces.gz")
	if err != nil {
		return fmt.Errorf("failed to create trace zipfile: %w", err)
	}
	defer file.Close()
	zipW := gzip.NewWriter(file)
	defer zipW.Close()

	b, err := proto.Marshal(traces)
	if err != nil {
		return fmt.Errorf("unable to marshal traces: %w", err)
	}
	_, err = zipW.Write(b)
	if err != nil {
		return fmt.Errorf("unable to write traces: %w", err)
	}

	return nil
}
