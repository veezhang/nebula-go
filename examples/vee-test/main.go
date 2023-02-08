/*
 *
 * Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 *
 */

package main

import (
	"crypto/tls"
	"fmt"
	"sync"
	"sync/atomic"

	nebula "github.com/vesoft-inc/nebula-go/v3"
)

var addresses = []string{
	"192.168.15.8",
	"192.168.15.9",
	"192.168.15.10",
}

const (
	// The default port of NebulaGraph 2.x is 9669.
	// 3699 is only for testing.
	port     = 9669
	username = "root"
	password = "nebula"
	useSSL   = false
	useHTTP2 = true
)

var count int64

func main() {
	var wg sync.WaitGroup
	for i := 0; i < 300; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			main1(i)
		}()
	}
	wg.Wait()
}

// Initialize logger
var log = nebula.DefaultLogger{}

func main1(num int) {
	address := addresses[num%len(addresses)]
	hostAddress := nebula.HostAddress{Host: address, Port: port}
	hostList := []nebula.HostAddress{hostAddress}
	// Create configs for connection pool using default values
	testPoolConfig := nebula.GetDefaultConf()
	testPoolConfig.UseHTTP2 = useHTTP2

	var sslConfig *tls.Config
	if useSSL {
		var err error
		sslConfig, err = nebula.GetDefaultSSLConfig(
			"./nebula-docker-compose/secrets/test.ca.pem",
			"./nebula-docker-compose/secrets/test.client.crt",
			"./nebula-docker-compose/secrets/test.client.key",
		)
		if err != nil {
			log.Fatal("Fail to create ssl config")
		}
		sslConfig.InsecureSkipVerify = true
	}

	// Initialize connection pool
	pool, err := nebula.NewSslConnectionPool(hostList, testPoolConfig, sslConfig, log)
	if err != nil {
		log.Fatal(fmt.Sprintf("Fail to initialize the connection pool, host: %s, port: %d, %s", address, port, err))
	}
	// Close all connections in the pool
	defer pool.Close()

	// Create session
	session, err := pool.GetSession(username, password)
	if err != nil {
		log.Fatal(fmt.Sprintf("Fail to create a new session from connection pool, username: %s, password: %s, %s",
			username, password, err))
	}
	// Release session and return connection back to connection pool
	defer session.Release()

	rs, err := session.Execute("USE sf100")
	if err != nil || !rs.IsSucceed() {
		log.Error(fmt.Sprintf("%d USE space failed %v", num, err))
		return
	}
	for {
		rs, err := session.Execute("GO 3 STEP FROM 2199023644421 OVER KNOWS yield KNOWS.creationDate")
		if err != nil {
			log.Error(fmt.Sprintf("%d %v %T", num, err, err))
			return
		}
		if !rs.IsSucceed() {
			log.Error(fmt.Sprintf("%d %d:%s", num, rs.GetErrorCode(), rs.GetErrorMsg()))
		}
		if n := atomic.AddInt64(&count, 1); n%1000 == 0 {
			log.Info(fmt.Sprintf("%d Execute count %d", num, n))
		}
	}
	fmt.Print("\n")
	log.Info("Nebula Go Client Basic Example Finished")
}
