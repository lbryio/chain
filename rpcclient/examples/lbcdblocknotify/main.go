package main

import (
	"flag"
	"fmt"
	"log"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/lbryio/lbcutil"
)

type stratumFlag []string

var (
	lbcdHomeDir = lbcutil.AppDataDir("lbcd", false)
	defaultCert = filepath.Join(lbcdHomeDir, "rpc.cert")
	stratumList stratumFlag
)
var (
	coinid      = flag.String("coinid", "1425", "Coin ID")
	stratumPass = flag.String("stratumpass", "", "Stratum server password")
	rpcserver   = flag.String("rpcserver", "localhost:9245", "LBCD RPC server")
	rpcuser     = flag.String("rpcuser", "rpcuser", "LBCD RPC username")
	rpcpass     = flag.String("rpcpass", "rpcpass", "LBCD RPC password")
	rpccert     = flag.String("rpccert", defaultCert, "LBCD RPC certificate")
	notls       = flag.Bool("notls", false, "Connect to LBCD with TLS disabled")
	run         = flag.String("run", "", "Run custom shell command")
	quiet       = flag.Bool("quiet", false, "Do not print logs")
)

func main() {

	flag.Var(&stratumList, "stratum", "--stratum=stratum1 --stratum=stratum2")
	flag.Parse()
	for _, stratum := range stratumList {
		go func(stratum string) {
			// Setup notification handler
			b := newBridge(stratum, *stratumPass, *coinid)

			if len(*run) > 0 {
				// Check if ccommand exists.
				strs := strings.Split(*run, " ")
				cmd := strs[0]
				_, err := exec.LookPath(cmd)
				if err != nil {
					log.Fatalf("ERROR: %s not found: %s", cmd, err)
				}
				b.customCmd = *run
			}

			// Start the eventt handler.
			go b.start()

			// Adaptater receives lbcd notifications, and emit events.
			adpt := adapter{b}

			client := newLbcdClient(*rpcserver, *rpcuser, *rpcpass, *notls, adpt)

			go func() {
				err := <-b.errorc
				log.Fatalf("ERROR: %s", err)
				client.Shutdown()
			}()
			// Wait until the client either shuts down gracefully (or the user
			// terminates the process with Ctrl+C).
			client.WaitForShutdown()
		}(stratum)
	}

	quit := make(chan bool)
	<-quit
}

func (f *stratumFlag) String() string {
	return fmt.Sprintf("%v", []string(*f))
}

func (f *stratumFlag) Set(value string) error {
	*f = append(*f, value)
	return nil
}
