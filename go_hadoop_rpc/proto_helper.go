package main

import (
	"code.google.com/p/goprotobuf/proto"
	"io"
)

func readProtoBytes(r io.Reader) ([]byte, error) {
	size, err := readVarint32(r)
	if err != nil {
		return nil, err
	}
	msg := make([]byte, size)
	_, err = io.ReadFull(r, msg)
	return msg, err
}

func readProto(r io.Reader, pmsg proto.Message) error {
	msg, err := readProtoBytes(r)
	if err != nil {
		return err
	}
	return proto.Unmarshal(msg, pmsg)
}
