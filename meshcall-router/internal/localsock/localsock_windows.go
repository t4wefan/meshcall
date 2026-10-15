//go:build windows

package localsock

import (
	"context"
	"net"

	"github.com/Microsoft/go-winio"
	"golang.org/x/sys/windows"
)

const (
	unixSupported  = false
	pipesSupported = true
)

// ListenPipe listens on \\.\pipe\NAME. Only the current user, SYSTEM and
// Administrators may connect: the same ownership a Unix socket gets from
// its file mode.
func ListenPipe(name string) (net.Listener, error) {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return nil, err
	}
	sddl := "D:P(A;;GA;;;" + user.User.Sid.String() + ")(A;;GA;;;SY)(A;;GA;;;BA)"
	return winio.ListenPipe(PipePrefix+name, &winio.PipeConfig{
		SecurityDescriptor: sddl, InputBufferSize: 64 * 1024, OutputBufferSize: 64 * 1024,
	})
}

// DialPipe connects to \\.\pipe\NAME.
func DialPipe(ctx context.Context, name string) (net.Conn, error) {
	return winio.DialPipeContext(ctx, PipePrefix+name)
}
