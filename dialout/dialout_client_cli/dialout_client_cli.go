// The dialout_client_cli program implements the telemetry publish client.
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"os"
	"os/signal"
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
<<<<<<< HEAD
	flag.DurationVar(&clientCfg.RetryInterval, "retry_interval", 30*time.Second, "Interval at which client tries to reconnect to destination servers")
=======
	flag.DurationVar(&clientCfg.RetryInterval, "retry_interval", 30*time.Second, "How long each connection attempt to a destination server lasts. Also the pause before reconnecting after a failed Publish or a session that lasted less, and before retrying a data source that cannot be opened. 0 or less means 1s")
>>>>>>> 29069e0 (NOS-23564: [dialout_client] Pace failed reads, sends and dials, and let every wait end on cancel (#401))
	flag.BoolVar(&clientCfg.Unidirectional, "unidirectional", true, "No repesponse from server is expected")
}

func main() {
	flag.Parse()
	ctx, cancel := context.WithCancel(context.Background())
	// Terminate on Ctrl+C
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		<-c
		cancel()
	}()
	log.V(1).Infof("Starting telemetry publish client")
	err := dc.DialOutRun(ctx, &clientCfg)
	log.V(1).Infof("Exiting telemetry publish client: %v", err)
	log.Flush()
}
