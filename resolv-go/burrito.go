package burrito

import (
	"context"

	burrito "github.com/akshayknarayan/burrito/resolv-go/proto"
	grpc "google.golang.org/grpc"
)

func Listen(root string, addr string) (string, error) {
	conn, err := grpc.Dial(root)
	if err != nil {
		return "", err
	}

	cc := burrito.NewConnectionClient(conn)
	ctx := context.Background()
	reply, err := cc.Listen(ctx, &burrito.ListenRequest{ServiceAddr: addr})
	if err != nil {
		return "", err

	}

	return reply.GetListenAddr(), nil
}
