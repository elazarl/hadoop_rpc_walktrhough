package main

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"encoding/binary"
	"fmt"
	"io"
	"testing"

	"./hadoop_common"
	"launchpad.net/gocheck"
)

var clientInput = []byte{
	//h   r     p     c
	0x68, 0x72, 0x70, 0x63,
	// version, service class, AuthProtocol
	0x09, 0x00, 0x00,
	// size of next two size delimited protobuf objets:
	// RpcRequestHeader and IpcConnectionContext
	0x00, 0x00, 0x00, 0x32, // = 50
	// varint encoding of RpcRequestHeader length
	0x1e, // = 30
	0x08, 0x02, 0x10, 0x00, 0x18, 0xfd, 0xff, 0xff, 0xff, 0x0f,
	0x22, 0x10, 0x87, 0xeb, 0x86, 0xd4, 0x9c, 0x95, 0x4c, 0x15,
	0x8a, 0xb0, 0xd7, 0xbc, 0x2e, 0xca, 0xca, 0x37, 0x28, 0x01,
	// varint encoding of IpcConnectionContext length
	0x12, // = 18
	0x12, 0x0a, 0x0a, 0x08, 0x65, 0x6c, 0x65, 0x69, 0x62, 0x6f,
	0x76, 0x69, 0x1a, 0x04, 0x70, 0x69, 0x6e, 0x67,
	// Size of RpcRequestHeader + RpcRequest protobuf objects
	0x00, 0x00, 0x00, 0x3f, // = 63
	// varint size of RpcRequest Header
	0x1a, // = 26
	0x08, 0x01, 0x10, 0x00, 0x18, 0x00, 0x22, 0x10, 0x87, 0xeb,
	0x86, 0xd4, 0x9c, 0x95, 0x4c, 0x15, 0x8a, 0xb0, 0xd7, 0xbc,
	0x2e, 0xca, 0xca, 0x37, 0x28, 0x00,
	// RPC Request writable. It's not size delimited
	// long - RPC version
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // = 2
	// utf8 string - protocol name
	0x00, 0x04, // string legnth = 4
	// p     i     n     g
	0x70, 0x69, 0x6e, 0x67,
	// utf8 string - method name
	0x00, 0x04, // string legnth = 4
	// p     i     n     g
	0x70, 0x69, 0x6e, 0x67,
	// long - client version
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // = 1
	// int - client method hash
	0xa0, 0xbd, 0x17, 0xcc,
	// int - parameter class length
	0x00, 0x00, 0x00, 0x00,
	// ping request
	0xff, 0xff, 0xff, 0xff,
}

var serverInput = []byte{
	// size of entire request
	0x00, 0x00, 0x00, 0x33,
	// varint size of RpcResponseHeader
	0x1a, // 16 + 10 = 26
	0x08, 0x00, 0x10, 0x00, 0x18, 0x09, 0x3a, 0x10, 0x9b, 0x19,
	0x9b, 0x41, 0x4d, 0x86, 0x42, 0xd7, 0x94, 0x79, 0x3f, 0x4b,
	0x16, 0xa0, 0x22, 0x7c, 0x40, 0x00,
	// Writable response
	// short - length of declared class
	0x00, 0x10,
	// j     a     v     a     .     l     a     n     g     .
	0x6a, 0x61, 0x76, 0x61, 0x2e, 0x6c, 0x61, 0x6e, 0x67, 0x2e,
	// S     t     r     i     n     g
	0x53, 0x74, 0x72, 0x69, 0x6e, 0x67,
	// short - length of value
	0x00, 0x04,
	// p     o     n     g
	0x70, 0x6f, 0x6e, 0x67,
}

func Test(t *testing.T) { gocheck.TestingT(t) }

type MySuite struct{}

var _ = gocheck.Suite(&MySuite{})

func readPayload(payload []byte, c *gocheck.C) {
	r := bytes.NewReader(payload)
	var rpcheader hadoop_common.RpcRequestHeaderProto
	panicOnErr(readProto(r, &rpcheader))
	fmt.Printf("%+#v", rpcheader)

	rpcVersion, err := readint64(r)
	panicOnErr(err)
	c.Check(rpcVersion, gocheck.Equals, int64(2))
	declaringClassProtocolName, err := readString(r)
	panicOnErr(err)
	c.Check(declaringClassProtocolName, gocheck.Equals, "ping")
	methodName, err := readString(r)
	panicOnErr(err)
	c.Check(methodName, gocheck.Equals, "ping")
	clientVersion, err := readint64(r)
	panicOnErr(err)
	c.Check(clientVersion, gocheck.Equals, int64(1))
	clientMethodHash, err := readint32(r)
	panicOnErr(err)
	fmt.Println("clientMethodHash:", clientMethodHash)
	parameterClassesLength, err := readint32(r)
	panicOnErr(err)
	fmt.Println("parameterClassesLength:", parameterClassesLength)
}

func (s *MySuite) TestClientInfo(c *gocheck.C) {
	r := bytes.NewReader(clientInput)
	preq, err := ReadRpcPrequel(r)
	panicOnErr(err)
	h := preq.Header
	c.Assert(string(h.Header[:]), gocheck.Equals, "hrpc")
	c.Check(h.Version >= 8, gocheck.Equals, true)
	c.Check(h.AuthProtocol, gocheck.Equals, NONE)
	c.Assert(h.ServiceClass, gocheck.Equals, byte(0))

	var payload_size int32
	panicOnErr(binary.Read(r, binary.BigEndian, &payload_size))
	fmt.Println("payload size: ", payload_size)
	payload := make([]byte, payload_size)
	_, err = io.ReadFull(r, payload)
	panicOnErr(err)
	readPayload(payload, c)
}
