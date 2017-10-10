//// Copyright 2015 Spring Rain Software Compnay LTD. All Rights Reserved.
//// Licensed under the MIT (MIT-LICENSE.txt) license.
package overseer

import (
	"context"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"github.com/wfxiang08/go_thrift/thrift"
	"net"
	"runtime/debug"
	"strings"
	"time"
)

type TGracefulServerSocket struct {
	listener      net.Listener
	clientTimeout time.Duration
}

func NewTGracefulServerSocket(listener net.Listener) *TGracefulServerSocket {
	return NewTGracefulServerSocketWithTimeout(listener, 0)
}

func NewTGracefulServerSocketWithTimeout(listener net.Listener, clientTimeout time.Duration) *TGracefulServerSocket {

	return &TGracefulServerSocket{listener: listener, clientTimeout: clientTimeout}
}

func (p *TGracefulServerSocket) Accept() (thrift.TTransport, error) {
	listener := p.listener
	if listener == nil {
		return nil, thrift.NewTTransportException(thrift.NOT_OPEN, "No underlying server socket")
	}

	conn, err := listener.Accept()
	if err != nil {
		return nil, thrift.NewTTransportExceptionFromError(err)
	}
	return thrift.NewTSocketFromConnTimeout(conn, p.clientTimeout), nil
}

func GracefulRunWithListener(listener net.Listener, processor thrift.TProcessor) {

	// transport := NewTGracefulServerSocketWithTimeout(listener, time.Second*5)
	transport := NewTGracefulServerSocket(listener)

	ch := make(chan thrift.TTransport, 4096)
	defer close(ch)

	go func() {
		for c := range ch {
			go func(c thrift.TTransport) {
				// 打印异常信息
				defer func() {
					c.Close()
					if e := recover(); e != nil {
						log.Errorf("panic in processor: %v: %s", e, debug.Stack())
					}
				}()

				ip := thrift.NewTBinaryProtocolTransport(thrift.NewTFramedTransport(c))
				op := thrift.NewTBinaryProtocolTransport(thrift.NewTFramedTransport(c))

				for true {
					defaultContext := context.Background()
					ok, err := processor.Process(defaultContext, ip, op)

					if err != nil {
						// 如果链路出现异常（必须结束)
						if err, ok := err.(thrift.TTransportException); ok && (err.TypeId() == thrift.END_OF_FILE ||
								strings.Contains(err.Error(), "use of closed network connection")) {
							return
						} else if err != nil {
							// 其他链路问题，直接报错，退出
							log.ErrorErrorf(err, "Error processing request, quit")
							return
						}

						// 如果是方法未知，则直接报错，然后跳过
						if err, ok := err.(thrift.TApplicationException); ok && err.TypeId() == thrift.UNKNOWN_METHOD {
							log.ErrorErrorf(err, "Error processing request, continue")
							return
						}

						log.ErrorErrorf(err, "Error processing request")
						// INTERNAL_ERROR --> ok: true 可以继续
						// PROTOCOL_ERROR --> ok: false
						// 非业务的错误都终止当前的连接
						return
					}

					// 其他情况下，ok == false，意味io过程中存在读写错误，connection上存在脏数据
					// 用户自定义的异常，必须继承自: *services.RpcException, 否则整个流程就容易出现问题
					if !ok {
						if err == nil {
							log.Printf("Process Not OK, stopped")
						}
						break
					}

				}
			}(c)
		}
	}()

	// Accept什么时候出错，出错之后如何处理呢?
	for {
		c, err := transport.Accept()
		if err != nil {
			break
		} else {
			ch <- c
		}
	}
}
