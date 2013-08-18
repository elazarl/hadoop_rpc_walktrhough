package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"io"
	"log"
	"net"

	"./hadoop_common"
)

func panicOnErr(err error) {
	if err != nil {
		panic(err)
	}
}

type AuthMethod byte

const (
	SIMPLE AuthMethod = 80 + iota
	KEREBROS
	DIGEST
	PLAIN
)

type AuthProtocol byte

const (
	NONE AuthProtocol = 0
	SASL = int8(-33)
)

type RpcHeader struct {
	Header [4]byte
	Version byte
	ServiceClass byte
	AuthProtocol AuthProtocol
}

type RpcPrequel struct {
	Header RpcHeader
	ReqHeader hadoop_common.RpcRequestHeaderProto
	ConnContext hadoop_common.IpcConnectionContextProto
}

func ReadRpcPrequel(r io.Reader) (preq *RpcPrequel, err error) {
	preq = new(RpcPrequel)
	preq.Header, err = ReadRpcHeader(r)
	readint32(r)
	if err != nil {
		return nil, err
	}
	if err := readProto(r, &preq.ReqHeader); err != nil {
		return nil, err
	}
	if err := readProto(r, &preq.ConnContext); err != nil {
		return nil, err
	}
	return preq, nil
}

func ReadRpcHeader(r io.Reader) (RpcHeader, error) {
	var header RpcHeader
	err := binary.Read(r, binary.BigEndian, &header)
	return header, err
}

func server(addr string) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalln("cannot listen to", addr, err)
	}
	for {
		c, err := l.Accept()
		if err != nil {
			log.Println("cannot accept connection", err)
			continue
		}
		go func() {
			defer c.Close()
			if _, err := ReadRpcPrequel(c); err != nil {
				log.Println("Cannot read prequel", err)
				return
			}
			for {
				size, err := readint32(c)
				payload := make([]byte, size)
				_, err = io.ReadFull(c, payload)

				r := bytes.NewReader(payload)
				var rpcheader hadoop_common.RpcRequestHeaderProto
				readProto(r, &rpcheader)
				rpcVersion, err := readint64(r)
				declaringClassProtocolName, err := readString(r)
				methodName, err := readString(r)
				clientVersion, err := readint64(r)
				clientMethodHash, err := readint32(r)
				parameterClassesLength, err := readint32(r)
				if err != nil {
					log.Println("error parsing client request", err)
					return
				}
				log.Println("rpc version", rpcVersion, "client version", clientVersion, "hash", clientMethodHash)
				log.Printf("%s: %s(%d args...)", declaringClassProtocolName, methodName, parameterClassesLength)
			}
		}()
	}
}

func main() {
	saddr := flag.String("server", "", "address to listen for RPC requests")
	flag.Parse()
	if *saddr != "" {
		server(*saddr)
	}
}

func writeVarint32(value int, w io.Writer) error {
	for {
		if (value &^ 0x7F) == 0 {
			if _, err := w.Write([]byte{byte(value)}); err != nil {
				return err
			}
			return nil
		} else {
			if _, err := w.Write([]byte{byte((value & 0x7F) | 0x80)}); err != nil {
				return err
			}
			value >>= 7;
		}
	}
	panic("unreachable")
}

func readByte(r io.Reader, perr *error) byte {
	if *perr != nil {
		return 0
	}
	var b [1]byte
	if _, *perr = r.Read(b[:]); *perr != nil {
		return 0
	}
	return b[0]
}

func readVarint32(r io.Reader) (int32, error) {
	var err error
	b := readByte(r, &err)
	if b & 0x80 == 0 {
		return int32(b), err
	}
	result := int32(b & 0x7F)
	if tmp := readByte(r, &err); tmp & 0x80 == 0 {
		return result | int32(tmp) << 7, err
	} else {
		result |= (int32(tmp) & 0x7F) << 7
	}
	if tmp := readByte(r, &err); tmp & 0x80 == 0 {
		return result | int32(tmp) << 14, err
	} else {
		result |= (int32(tmp) & 0x7F) << 14
	}
	if tmp := readByte(r, &err); tmp & 0x80 == 0 {
		return result | int32(tmp) << 21, err
	} else {
		result |= (int32(tmp) & 0x7F) << 21
	}
	tmp := readByte(r, &err)
	result |= int32(tmp) << 28
	if tmp & 0x80 != 0 {
		for i := 0; i < 5; i++ {
			if readByte(r, &err) & 0x80 == 0 {
				return result, err
			}
		}
		return 0, errors.New("malformed varint32")
	}
	return result, err
}

