// The dialout_client_cli program implements the telemetry publish client.
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	dc "github.com/sonic-net/sonic-gnmi/dialout/dialout_client"

	log "github.com/golang/glog"
	gpb "github.com/openconfig/gnmi/proto/gnmi"
)

var (
	clientCfg = dc.ClientConfig{
		SrcIp:          "",
		RetryInterval:  30 * time.Second,
		Encoding:       gpb.Encoding_JSON_IETF,
		Unidirectional: true,
		TLS:            &tls.Config{MinVersion: tls.VersionTLS13},
	}
)

func init() {
	flag.StringVar(&clientCfg.TLS.ServerName, "server_name", "", "When set, use this hostname to verify server certificate during TLS handshake.")
	flag.BoolVar(&clientCfg.TLS.InsecureSkipVerify, "insecure", false, "When set, client will not verify the server certificate during TLS handshake.")
	flag.DurationVar(&clientCfg.RetryInterval, "retry_interval", 30*time.Second, "Interval at which client tries to reconnect to destination servers")
	flag.BoolVar(&clientCfg.Unidirectional, "unidirectional", true, "No repesponse from server is expected")
}

// stopSignals are the signals that stop the client cleanly. SIGTERM is the
// signal a service manager stops a process with, and without a handler it
// kills the process before DialOutRun can return and remove its rows.
// SIGQUIT is left to Go, which dumps every goroutine's stack and exits.
func stopSignals() []os.Signal {
	return []os.Signal{os.Interrupt, syscall.SIGTERM, syscall.SIGHUP}
}

// cancelOnStop calls cancel when one of stopSignals arrives, so DialOutRun
// returns and its rows are removed. Only the first is handled: a second
// stop signal gets the default action and ends the process, so a clean stop
// that hangs can still be stopped.
func cancelOnStop(cancel func()) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, stopSignals()...)
	go func() {
		<-c
		signal.Stop(c)
		cancel()
	}()
}

func main() {
	flag.Parse()
	ctx, cancel := context.WithCancel(context.Background())
	cancelOnStop(cancel)
	log.V(1).Infof("Starting telemetry publish client")
	err := dc.DialOutRun(ctx, &clientCfg)
	log.V(1).Infof("Exiting telemetry publish client: %v", err)
	log.Flush()
}
