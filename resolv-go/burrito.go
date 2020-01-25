package burrito

import (
	"context"

	rpc "github.com/akshayknarayan/burrito/resolv-go/proto"
	grpc "google.golang.org/grpc"
)

func getConnClient(root string) (rpc.ConnectionClient, error) {
	conn, err := grpc.Dial(root)
	if err != nil {
		return nil, err
	}

	return rpc.NewConnectionClient(conn), nil

}

func Listen(root string, addr string) (string, error) {
	cc, err := getConnClient(root)
	if err != nil {
		return "", err
	}

	ctx := context.Background()
	reply, err := cc.Listen(ctx, &rpc.ListenRequest{ServiceAddr: addr})
	if err != nil {
		return "", err

	}

	return reply.GetListenAddr(), nil
}

func Connect(root string, addr string) (string, error) {
	cc, err := getConnClient(root)
	if err != nil {
		return "", err
	}

	ctx := context.Background()
	reply, err := cc.Open(ctx, &rpc.OpenRequest{DstAddr: addr})
	if err != nil {
		return "", err

	}

	return reply.GetSendAddr(), nil
}
