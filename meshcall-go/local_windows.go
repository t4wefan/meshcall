//go:build windows

package meshcall

import (
	"context"
	"net"

	"github.com/Microsoft/go-winio"
	"golang.org/x/sys/windows"
)

const pipesSupported = true

// listenPipe admits only the current user, SYSTEM and Administrators, the
// ownership a Unix socket gets from its file mode.
func listenPipe(name string) (net.Listener, error) {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return nil, err
	}
	sddl := "D:P(A;;GA;;;" + user.User.Sid.String() + ")(A;;GA;;;SY)(A;;GA;;;BA)"
	return winio.ListenPipe(pipePrefix+name, &winio.PipeConfig{SecurityDescriptor: sddl, InputBufferSize: 64 * 1024, OutputBufferSize: 64 * 1024})
}

func dialPipe(ctx context.Context, name string) (net.Conn, error) {
	return winio.DialPipeContext(ctx, pipePrefix+name)
}
