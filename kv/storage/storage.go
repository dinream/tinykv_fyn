package storage

import (
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// Storage 代表着 TinyKV 面向内部的服务器（存储引擎）部分, 处理来自其他服务器的发送和接收
// TinyKV 节点。 作为该职责的一部分，它还读取数据并将其写入磁盘（或半永久内存）。
type Storage interface {
	Start() error
	Stop() error
	Write(ctx *kvrpcpb.Context, batch []Modify) error
	Reader(ctx *kvrpcpb.Context) (StorageReader, error)
}

type StorageReader interface {
	// When the key doesn't exist, return nil for the value
	GetCF(cf string, key []byte) ([]byte, error)
	IterCF(cf string) engine_util.DBIterator
	Close()
}
