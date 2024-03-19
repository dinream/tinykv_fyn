package config

import (
	"fmt"
	"os"
	"time"

	"github.com/pingcap-incubator/tinykv/log"
)

// 配置参数 结构体
type Config struct {
	StoreAddr     string // 存储地址
	Raft          bool   // Raft 开关
	SchedulerAddr string // 调度程序地址
	LogLevel      string // 日志级别

	DBPath string // 存储数据的目录。应该存在并且可写。

	// raft_base_tick_interval 是基本刻度间隔（毫秒）。
	RaftBaseTickInterval     time.Duration
	RaftHeartbeatTicks       int // 心跳时间
	RaftElectionTimeoutTicks int // 选举超时时间

	// Raft 垃圾自动回收间隔 (ms). GC：garbage collection
	RaftLogGCTickInterval time.Duration
	// Raft 的日志条目上限, 超出时触发 gc.
	RaftLogGcCountLimit uint64

	// 区域拆分检查时间间隔（ms）负载均衡要求，数据过大或者频繁访问时拆分区块
	SplitRegionCheckTickInterval time.Duration
	// delay time before deleting a stale peer 删除陈旧对等点之前的延迟时间
	SchedulerHeartbeatTickInterval      time.Duration // 调度器心跳间隔时间
	SchedulerStoreHeartbeatTickInterval time.Duration // 存储心跳间隔时间

	// When region [a,e) size meets regionMaxSize, it will be split into
	// several regions [a,b), [b,c), [c,d), [d,e). And the size of [a,b),
	// [b,c), [c,d) will be regionSplitSize (maybe a little larger).
	RegionMaxSize   uint64 // 区域最大大小，拆分标准
	RegionSplitSize uint64 // 区域拆分后的 标准 大小
}

func (c *Config) Validate() error {
	if c.RaftHeartbeatTicks == 0 {
		return fmt.Errorf("heartbeat tick must greater than 0")
	}

	if c.RaftElectionTimeoutTicks != 10 {
		log.Warnf("Election timeout ticks needs to be same across all the cluster, " +
			"otherwise it may lead to inconsistency.")
	}

	if c.RaftElectionTimeoutTicks <= c.RaftHeartbeatTicks {
		return fmt.Errorf("election tick must be greater than heartbeat tick.")
	}

	return nil
}

const (
	KB uint64 = 1024
	MB uint64 = 1024 * 1024
)

/**
* 获取默认或者自定义的日志级别
 */
func getLogLevel() (logLevel string) {
	logLevel = "info"
	if l := os.Getenv("LOG_LEVEL"); len(l) != 0 {
		logLevel = l
	}
	return
}

func NewDefaultConfig() *Config {
	return &Config{
		SchedulerAddr:            "127.0.0.1:2379",
		StoreAddr:                "127.0.0.1:20160",
		LogLevel:                 getLogLevel(),
		Raft:                     true,
		RaftBaseTickInterval:     1 * time.Second,
		RaftHeartbeatTicks:       2,
		RaftElectionTimeoutTicks: 10,
		RaftLogGCTickInterval:    10 * time.Second,
		// Assume the average size of entries is 1k.
		RaftLogGcCountLimit:                 128000,
		SplitRegionCheckTickInterval:        10 * time.Second,
		SchedulerHeartbeatTickInterval:      10 * time.Second,
		SchedulerStoreHeartbeatTickInterval: 10 * time.Second,
		RegionMaxSize:                       144 * MB,
		RegionSplitSize:                     96 * MB,
		DBPath:                              "/tmp/badger",
	}
}

func NewTestConfig() *Config {
	return &Config{
		LogLevel:                 getLogLevel(),
		Raft:                     true,
		RaftBaseTickInterval:     50 * time.Millisecond,
		RaftHeartbeatTicks:       2,
		RaftElectionTimeoutTicks: 10,
		RaftLogGCTickInterval:    50 * time.Millisecond,
		// Assume the average size of entries is 1k.
		RaftLogGcCountLimit:                 128000,
		SplitRegionCheckTickInterval:        100 * time.Millisecond,
		SchedulerHeartbeatTickInterval:      100 * time.Millisecond,
		SchedulerStoreHeartbeatTickInterval: 500 * time.Millisecond,
		RegionMaxSize:                       144 * MB,
		RegionSplitSize:                     96 * MB,
		DBPath:                              "/tmp/badger",
	}
}
