// Copyright (c) 2018-2020 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// Tool dumpfeedb can be used to dump the internal state of the buckets of an
// estimator's feedb so that it can be externally analyzed.
package main

import (
	"errors"
	"fmt"
	"os"
	"path"

	"github.com/btcsuite/btclog"
	flags "github.com/jessevdk/go-flags"
	"github.com/lbryio/lbcd/fees"
	"github.com/lbryio/lbcutil"
)

type config struct {
	DB string `short:"b" long:"db" description:"Path to fee database"`
}

var feesLog = btclog.NewBackend(os.Stdout).Logger("FEES")

func main() {
	cfg := config{
		DB: path.Join(lbcutil.AppDataDir("lbcd", false), "data", "mainnet", "feesdb"),
	}

	fees.UseLogger(feesLog)
	parser := flags.NewParser(&cfg, flags.Default)
	_, err := parser.Parse()
	if err != nil {
		var e *flags.Error
		if !errors.As(err, &e) || e.Type != flags.ErrHelp {
			parser.WriteHelp(os.Stderr)
		}
		return
	}

	ecfg := fees.EstimatorConfig{
		DatabaseFile:         cfg.DB,
		ReplaceBucketsOnLoad: true,
		MinBucketFee:         1,
		MaxBucketFee:         2,
		FeeRateStep:          fees.DefaultFeeRateStep,
	}
	est, err := fees.NewEstimator(&ecfg)
	if err != nil {
		panic(err)
	}

	fmt.Println(est.DumpBuckets())
}
