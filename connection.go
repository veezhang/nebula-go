/*
 *
 * Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 *
 */

package nebula_go

import (
	"crypto/tls"
	"fmt"
	"math"
	"net"
	"strconv"
	"time"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	"github.com/vesoft-inc/nebula-go/v3/nebula"
	"github.com/vesoft-inc/nebula-go/v3/nebula/graph"
)

type connection struct {
	severAddress HostAddress
	timeout      time.Duration
	returnedAt   time.Time // the connection was created or returned.
	sslConfig    *tls.Config
	graph        *graph.GraphServiceClient
}

func newConnection(severAddress HostAddress) *connection {
	return &connection{
		severAddress: severAddress,
		timeout:      0 * time.Millisecond,
		returnedAt:   time.Now(),
		sslConfig:    nil,
		graph:        nil,
	}
}

// open opens a transport for the connection
// if sslConfig is not nil, an SSL transport will be created
func (cn *connection) open(hostAddress HostAddress, timeout time.Duration, sslConfig *tls.Config) error {
	ip := hostAddress.Host
	port := hostAddress.Port
	newAdd := net.JoinHostPort(ip, strconv.Itoa(port))
	cn.timeout = timeout
	bufferSize := 128 << 10
	frameMaxLength := uint32(math.MaxUint32)

	var err error
	var sock thrift.Transport
	if sslConfig != nil {
		sock, err = thrift.NewSSLSocketTimeout(newAdd, sslConfig, timeout)
	} else {
		sock, err = thrift.NewSocket(thrift.SocketAddr(newAdd), thrift.SocketTimeout(timeout))
	}
	if err != nil {
		return fmt.Errorf("failed to create a net.Conn-backed Transport,: %s", err.Error())
	}

	// Set transport buffer
	bufferedTranFactory := thrift.NewBufferedTransportFactory(bufferSize)
	transport := thrift.NewFramedTransportMaxLength(bufferedTranFactory.GetTransport(sock), frameMaxLength)
	pf := thrift.NewBinaryProtocolFactoryDefault()
	cn.graph = graph.NewGraphServiceClientFactory(transport, pf)
	if err = cn.graph.Open(); err != nil {
		return fmt.Errorf("failed to open transport, error: %s", err.Error())
	}
	if !cn.graph.IsOpen() {
		return fmt.Errorf("transport is off")
	}
	return cn.verifyClientVersion()
}

func (cn *connection) verifyClientVersion() error {
	req := graph.NewVerifyClientVersionReq()
	resp, err := cn.graph.VerifyClientVersion(req)
	if err != nil {
		cn.close()
		return fmt.Errorf("failed to verify client version: %s", err.Error())
	}
	if resp.GetErrorCode() != nebula.ErrorCode_SUCCEEDED {
		return fmt.Errorf("incompatible version between client and server: %s", string(resp.GetErrorMsg()))
	}
	return nil
}

// reopen reopens the current connection.
// Because the code generated by Fbthrift does not handle the seqID,
// the message will be dislocated when the timeout occurs, resulting in unexpected response.
// When the timeout occurs, the connection will be reopened to avoid the impact of the message.
func (cn *connection) reopen() error {
	cn.close()
	return cn.open(cn.severAddress, cn.timeout, cn.sslConfig)
}

// Authenticate
func (cn *connection) authenticate(username, password string) (*graph.AuthResponse, error) {
	resp, err := cn.graph.Authenticate([]byte(username), []byte(password))
	if err != nil {
		err = fmt.Errorf("authentication fails, %s", err.Error())
		if e := cn.graph.Close(); e != nil {
			err = fmt.Errorf("fail to close transport, error: %s", e.Error())
		}
		return nil, err
	}
	if resp.ErrorCode != nebula.ErrorCode_SUCCEEDED {
		return nil, fmt.Errorf("fail to authenticate, error: %s", resp.ErrorMsg)
	}
	return resp, err
}

func (cn *connection) execute(sessionID int64, stmt string) (*graph.ExecutionResponse, error) {
	return cn.executeWithParameter(sessionID, stmt, map[string]*nebula.Value{})
}

func (cn *connection) executeWithParameter(sessionID int64, stmt string, params map[string]*nebula.Value) (*graph.ExecutionResponse, error) {
	resp, err := cn.graph.ExecuteWithParameter(sessionID, []byte(stmt), params)
	if err != nil {
		// reopen the connection if timeout
		if _, ok := err.(thrift.TransportException); ok {
			if err.(thrift.TransportException).TypeID() == thrift.TIMED_OUT {
				reopenErr := cn.reopen()
				if reopenErr != nil {
					return nil, reopenErr
				}
				return cn.graph.ExecuteWithParameter(sessionID, []byte(stmt), params)
			}
		}
	}

	return resp, err
}

func (cn *connection) executeJson(sessionID int64, stmt string) ([]byte, error) {
	return cn.ExecuteJsonWithParameter(sessionID, stmt, map[string]*nebula.Value{})
}

func (cn *connection) ExecuteJsonWithParameter(sessionID int64, stmt string, params map[string]*nebula.Value) ([]byte, error) {
	jsonResp, err := cn.graph.ExecuteJsonWithParameter(sessionID, []byte(stmt), params)
	if err != nil {
		// reopen the connection if timeout
		if _, ok := err.(thrift.TransportException); ok {
			if err.(thrift.TransportException).TypeID() == thrift.TIMED_OUT {
				reopenErr := cn.reopen()
				if reopenErr != nil {
					return nil, reopenErr
				}
				return cn.graph.ExecuteJsonWithParameter(sessionID, []byte(stmt), params)
			}
		}
	}

	return jsonResp, err
}

// Check connection to host address
func (cn *connection) ping() bool {
	_, err := cn.execute(0, "YIELD 1")
	return err == nil
}

// Check connection to host address
func (cn *connection) pingWithParameter() bool {
	_, err := cn.executeWithParameter(0, "YIELD 1", nil)
	return err == nil
}

// Sign out and release seesin ID
func (cn *connection) signOut(sessionID int64) error {
	// Release session ID to graphd
	return cn.graph.Signout(sessionID)
}

// Update returnedAt for cleaner
func (cn *connection) release() {
	cn.returnedAt = time.Now()
}

// Close transport
func (cn *connection) close() {
	cn.graph.Close()
}
