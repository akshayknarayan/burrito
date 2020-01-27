package burrito

import (
	"context"
	"fmt"
	"net"
	"time"

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

// Get a function that resolves address strings
// net.Conn is an interface, so we can return TCP/UnixStream as appropriate here.
func BurritoDialer(burrito_root string) func(addr string, timeout time.Duration) (net.Conn, error) {
	return func(addr string, timeout time.Duration) (net.Conn, error) {
		// ask burrito-ctl to resolve string
		addr_type, addr, err := Connect(burrito_root, addr)
		if err != nil {
			return nil, err
		}

		var network string
		if addr_type == rpc.OpenReply_UNIX {
			network = "unix"
		} else if addr_type == rpc.OpenReply_TCP {
			network = "tcp"
		} else {
			return nil, fmt.Errorf("Unknown address type: %v", addr_type)
		}

		// connect to resulting address
		conn, err := (&net.Dialer{Timeout: timeout}).Dial(network, addr)
		if err != nil {
			return nil, err
		}

		return conn, nil
	}
}
