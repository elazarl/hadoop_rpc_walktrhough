package main

import (
	"encoding/binary"
	"io"
)

func readint16(r io.Reader) (int16, error) {
	var d int16
	err := binary.Read(r, binary.BigEndian, &d)
	return d, err
}

func readint32(r io.Reader) (int32, error) {
	var d int32
	err := binary.Read(r, binary.BigEndian, &d)
	return d, err
}

func readint64(r io.Reader) (int64, error) {
	var d int64
	err := binary.Read(r, binary.BigEndian, &d)
	return d, err
}

func readString(r io.Reader) (string, error) {
	size, err := readint16(r)
	if err != nil {
		return "", err
	}
	s := make([]byte, size)
	if _, err := io.ReadFull(r, s); err != nil {
		return "", err
	}
	return string(s), nil
}
