package common_utils

import (
	"fmt"
	"syscall"
)

// BindToDevice returns a socket control function that binds the socket to
// the network device dev with SO_BINDTODEVICE, so a VRF device steers its
// traffic. It fits net.ListenConfig.Control and net.Dialer.Control.
func BindToDevice(dev string) func(network, address string, c syscall.RawConn) error {
	return func(network, address string, c syscall.RawConn) error {
		var err error
		ctrlErr := c.Control(func(fd uintptr) {
			err = syscall.SetsockoptString(int(fd), syscall.SOL_SOCKET, syscall.SO_BINDTODEVICE, dev)
		})
		if ctrlErr != nil {
			return ctrlErr
		}
		if err != nil {
			return fmt.Errorf("failed to bind socket to VRF %s: %v", dev, err)
		}
		return nil
	}
}
