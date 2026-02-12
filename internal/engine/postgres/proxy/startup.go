package proxy

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
)

// buildStartupMessage constructs a PostgreSQL StartupMessage.
// Format: length(4) + protocol_version(4) + key\0value\0 pairs + \0
func buildStartupMessage(user, dbname string) []byte {
	var buf []byte

	// Placeholder for length (filled in at the end).
	buf = append(buf, 0, 0, 0, 0)

	// Protocol version 3.0 = 196608 (0x00030000).
	var version [4]byte
	binary.BigEndian.PutUint32(version[:], 196608)
	buf = append(buf, version[:]...)

	// Parameters: user, database.
	buf = append(buf, "user\x00"...)
	buf = append(buf, user...)
	buf = append(buf, 0)
	buf = append(buf, "database\x00"...)
	buf = append(buf, dbname...)
	buf = append(buf, 0)

	// Terminating null byte.
	buf = append(buf, 0)

	// Fill in length (includes itself).
	binary.BigEndian.PutUint32(buf[:4], uint32(len(buf)))

	return buf
}

// buildPasswordMessage constructs a PG PasswordMessage.
// Format: type('p') + length(4) + password\0
func buildPasswordMessage(password string) []byte {
	payloadLen := len(password) + 1 // password + null terminator
	msgLen := 4 + payloadLen        // length field includes itself

	buf := make([]byte, 1+4+payloadLen)
	buf[0] = 'p'
	binary.BigEndian.PutUint32(buf[1:5], uint32(msgLen))
	copy(buf[5:], password)
	// buf[len(buf)-1] is already 0 from make()

	return buf
}

// buildMD5PasswordMessage constructs a PG MD5 PasswordMessage.
// MD5 auth: "md5" + md5(md5(password + user) + salt)
func buildMD5PasswordMessage(user, password string, salt []byte) []byte {
	inner := md5.Sum([]byte(password + user))
	innerHex := fmt.Sprintf("%x", inner)

	outer := md5.Sum(append([]byte(innerHex), salt...))
	outerHex := "md5" + fmt.Sprintf("%x", outer)

	return buildPasswordMessage(outerHex)
}
