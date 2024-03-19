package main

import (
	"flag"
	"net"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/server"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/standalone_storage"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var (
	schedulerAddr = flag.String("scheduler", "", "scheduler address")
	storeAddr     = flag.String("addr", "", "store address")
	dbPath        = flag.String("path", "", "directory path of db")
	logLevel      = flag.String("loglevel", "", "the level of log")
)

func main() {
	flag.Parse()                      // 参数解析
	conf := config.NewDefaultConfig() // 默认配置
	// 自定义配置
	if *schedulerAddr != "" {
		conf.SchedulerAddr = *schedulerAddr
	}
	if *storeAddr != "" {
		conf.StoreAddr = *storeAddr
	}
	if *dbPath != "" {
		conf.DBPath = *dbPath
	}
	if *logLevel != "" {
		conf.LogLevel = *logLevel
	}
	//* 日志配置
	log.SetLevelByString(conf.LogLevel)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.Infof("Server started with conf %+v", conf)
	//* 配置服务器
	// 面向内的：存储引擎配置
	var storage storage.Storage
	if conf.Raft {
		storage = raft_storage.NewRaftStorage(conf)
	} else {
		storage = standalone_storage.NewStandAloneStorage(conf)
	}
	if err := storage.Start(); err != nil {
		log.Fatal(err)
	}
	server := server.NewServer(storage)

	// 面向外的 服务器
	// 配置 grpc
	// 配置连接保活策略：用于在 gRPC 客户端和服务器之间维持活动连接的稳定性和可靠性。通过定期发送心跳（Ping）消息来确保连接处于活动状态
	var alivePolicy = keepalive.EnforcementPolicy{
		MinTime:             2 * time.Second, // If a client pings more than once every 2 seconds, terminate the connection
		PermitWithoutStream: true,            // Allow pings even when there are no active streams 即使没有正在进行的 gRPC 请求或响应，客户端仍然可以发送心跳消息以维持连接的活动状态。
	}

	grpcServer := grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(alivePolicy),
		grpc.InitialWindowSize(1<<30),     // 初始流控窗口大小：1g
		grpc.InitialConnWindowSize(1<<30), // 初始连接流控窗口
		grpc.MaxRecvMsgSize(10*1024*1024), // 单个消息大小限制
	)
	// 注册 TinyKV 服务器（grpc+server）
	tinykvpb.RegisterTinyKvServer(grpcServer, server) // grpcServer 是一个 gRPC 服务器对象，而 server 是一个实现了 gRPC 服务接口的对象。
	listenAddr := conf.StoreAddr[strings.IndexByte(conf.StoreAddr, ':'):]
	l, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatal(err)
	}
	handleSignal(grpcServer)
	// grpc 服务窗口
	err = grpcServer.Serve(l) // 启动一个 grpc 服务，阻塞监听
	if err != nil {
		log.Fatal(err)
	}
	log.Info("Server stopped.")
}

// 接收一定的信号之后 推出grpc 服务
func handleSignal(grpcServer *grpc.Server) {
	sigCh := make(chan os.Signal, 1) // 缓冲区大小为 1 的信号通道
	signal.Notify(sigCh,             // 将需要监听的操作系统信号注册到 sigCh 通道上。
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() { // go func() {...}()：这是一个匿名函数，并通过 go 关键字在新的 Goroutine 中异步执行。
		sig := <-sigCh // 从 sigCh 通道接收信号。这里的代码会阻塞，直到有信号被发送到 sigCh 通道。
		log.Infof("Got signal [%s] to exit.", sig)
		grpcServer.Stop()
	}()
}
