// Copyright 2026 Nexthop Systems Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

// A service manager stops the client with SIGTERM: it must stop it cleanly,
// as Ctrl+C does, so the rows the client published are removed.
func TestStopSignals(t *testing.T) {
	want := map[os.Signal]bool{os.Interrupt: true, syscall.SIGTERM: true, syscall.SIGHUP: true}
	got := map[os.Signal]bool{}
	for _, s := range stopSignals() {
		got[s] = true
	}
	for s := range want {
		if !got[s] {
			t.Errorf("%v does not stop the client", s)
		}
	}
	if got[syscall.SIGQUIT] {
		t.Errorf("SIGQUIT is trapped; Go's stack dump on SIGQUIT is lost")
	}
}

// The handler main installs turns a real SIGTERM into a cancel.
func TestCancelOnStopSIGTERM(t *testing.T) {
	cancelled := make(chan struct{})
	cancelOnStop(func() { close(cancelled) })
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("kill: %v", err)
	}
	select {
	case <-cancelled:
	case <-time.After(3 * time.Second):
		t.Fatalf("SIGTERM did not cancel the run")
	}
}

// A second stop signal ends a client whose clean stop hangs: run as a child
// whose cancel never returns, the first SIGTERM is handled and the second
// kills it.
func TestSecondSignalEndsHungStop(t *testing.T) {
	if os.Getenv("DIALOUT_HUNG_STOP_CHILD") == "1" {
		cancelOnStop(func() { select {} })
		os.Stdout.WriteString("ready\n")
		select {}
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestSecondSignalEndsHungStop$")
	cmd.Env = append(os.Environ(), "DIALOUT_HUNG_STOP_CHILD=1")
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 6)
	if _, err := out.Read(buf); err != nil {
		t.Fatalf("child did not start: %v", err)
	}
	cmd.Process.Signal(syscall.SIGTERM)
	time.Sleep(300 * time.Millisecond) // the first is handled; the child keeps running
	cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		ws, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
		if !ok || !ws.Signaled() || ws.Signal() != syscall.SIGTERM {
			t.Errorf("child ended with %v, want killed by the second SIGTERM", err)
		}
	case <-time.After(3 * time.Second):
		cmd.Process.Kill()
		t.Fatalf("a second SIGTERM did not end a client whose stop hangs")
	}
}
