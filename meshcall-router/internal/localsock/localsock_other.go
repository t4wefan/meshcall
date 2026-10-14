//go:build !windows

package localsock

import (
	"context"
	"net"
)

const (
	unixSupported  = true
	pipesSupported = false
)

// ListenPipe fails: named pipes are the Windows local transport.
func ListenPipe(string) (net.Listener, error) { return nil, ErrPipeOffWindows }

// DialPipe fails: named pipes are the Windows local transport.
func DialPipe(context.Context, string) (net.Conn, error) { return nil, ErrPipeOffWindows }
