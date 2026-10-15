//go:build !windows

package meshcall

import (
	"context"
	"net"
)

const pipesSupported = false

func listenPipe(string) (net.Listener, error) { return nil, errPipeOffWindows }

func dialPipe(context.Context, string) (net.Conn, error) { return nil, errPipeOffWindows }
