package burrito

import (
	"context"

	burrito "github.com/akshayknarayan/burrito/resolv-go/proto"
	grpc "google.golang.org/grpc"
)

func getConnClient(root string) (burrito.ConnectionClient, error) {
	conn, err := grpc.Dial(root)
	if err != nil {
		return nil, err
	}

	return burrito.NewConnectionClient(conn), nil

}

func Listen(root string, addr string) (string, error) {
	cc, err := getConnClient(root)
	if err != nil {
		return "", err
	}

	ctx := context.Background()
	reply, err := cc.Listen(ctx, &burrito.ListenRequest{ServiceAddr: addr})
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
	reply, err := cc.Open(ctx, &burrito.OpenRequest{DstAddr: addr})
	if err != nil {
		return "", err

	}

	return reply.GetSendAddr(), nil
}
