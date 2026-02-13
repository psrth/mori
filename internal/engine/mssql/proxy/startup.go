package proxy

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"time"
)

// PRELOGIN encryption negotiation values.
const (
	preloginEncryptOff    byte = 0x00
	preloginEncryptOn     byte = 0x01
	preloginEncryptNotSup byte = 0x02
	preloginEncryptReq    byte = 0x03
)

// relayHandshake relays the TDS handshake between client and production server.
// TDS handshake flow:
// 1. Client -> Server: PRELOGIN (type 0x12)
// 2. Server -> Client: PRELOGIN response (type 0x12)
// 3. Client -> Server: LOGIN7 (type 0x10)
// 4. Server -> Client: Login response (type 0x04, contains LOGINACK + ENVCHANGE + DONE tokens)
func relayHandshake(clientConn, prodConn net.Conn) error {
	// 1. Read PRELOGIN from client.
	preloginReq, err := readTDSMessage(clientConn)
	if err != nil {
		return fmt.Errorf("reading client prelogin: %w", err)
	}
	if len(preloginReq) == 0 {
		return fmt.Errorf("empty prelogin from client")
	}

	// Strip encryption and MARS from client's PRELOGIN before forwarding to prod.
	// Encryption: prevents TLS-within-TDS negotiation that the proxy cannot relay.
	// MARS: the proxy sends its own SQL_BATCH messages for internal queries
	// (shadow reads, prod reads during merge) and cannot generate MARS transaction
	// descriptor headers. Disabling MARS keeps the TDS session simple.
	for _, pkt := range preloginReq {
		if pkt.Type == typePrelogin {
			setPreloginEncryption(pkt.Payload, preloginEncryptNotSup)
			setPreloginOption(pkt.Payload, 0x04, 0x00) // MARS off (preloginMARS = 4)
			copy(pkt.Raw[tdsHeaderSize:], pkt.Payload)
		}
	}

	// Forward to prod.
	if err := writeTDSPackets(prodConn, preloginReq); err != nil {
		return fmt.Errorf("forwarding prelogin to prod: %w", err)
	}

	// 2. Read PRELOGIN response from prod.
	preloginResp, err := readTDSMessage(prodConn)
	if err != nil {
		return fmt.Errorf("reading prod prelogin response: %w", err)
	}

	// Strip encryption and MARS from prod's response before forwarding to client.
	for _, pkt := range preloginResp {
		if pkt.Type == typePrelogin {
			setPreloginEncryption(pkt.Payload, preloginEncryptNotSup)
			setPreloginOption(pkt.Payload, 0x04, 0x00) // MARS off (preloginMARS = 4)
			copy(pkt.Raw[tdsHeaderSize:], pkt.Payload)
		}
	}

	// Forward to client.
	if err := writeTDSPackets(clientConn, preloginResp); err != nil {
		return fmt.Errorf("forwarding prelogin response to client: %w", err)
	}

	// 3. Read LOGIN7 from client.
	loginReq, err := readTDSMessage(clientConn)
	if err != nil {
		return fmt.Errorf("reading client login7: %w", err)
	}
	if len(loginReq) == 0 {
		return fmt.Errorf("empty login7 from client")
	}

	// Forward to prod.
	if err := writeTDSPackets(prodConn, loginReq); err != nil {
		return fmt.Errorf("forwarding login7 to prod: %w", err)
	}

	// 4. Read login response from prod (tabular result containing LOGINACK, ENVCHANGE, DONE).
	loginResp, err := readTDSMessage(prodConn)
	if err != nil {
		return fmt.Errorf("reading prod login response: %w", err)
	}

	// Check for login failure by looking at the first token.
	if len(loginResp) > 0 && len(loginResp[0].Payload) > 0 {
		firstToken := loginResp[0].Payload[0]
		if firstToken == tokenError {
			// Forward the error to client and return error.
			writeTDSPackets(clientConn, loginResp)
			return fmt.Errorf("prod login failed (error token in response)")
		}
	}

	// Forward to client.
	if err := writeTDSPackets(clientConn, loginResp); err != nil {
		return fmt.Errorf("forwarding login response to client: %w", err)
	}

	return nil
}

// connectShadow establishes a raw TDS connection to the Shadow MSSQL server
// and performs the TDS handshake with known credentials.
func connectShadow(shadowAddr, dbName string) (net.Conn, error) {
	shadowConn, err := net.DialTimeout("tcp", shadowAddr, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial shadow %s: %w", shadowAddr, err)
	}

	// 1. Send PRELOGIN to shadow.
	preloginPayload := buildPreloginPayload()
	preloginPkt := buildTDSPacket(typePrelogin, statusEOM, preloginPayload)
	if _, err := shadowConn.Write(preloginPkt); err != nil {
		shadowConn.Close()
		return nil, fmt.Errorf("sending shadow prelogin: %w", err)
	}

	// 2. Read PRELOGIN response and check encryption requirement.
	preloginResp, err := readTDSMessage(shadowConn)
	if err != nil {
		shadowConn.Close()
		return nil, fmt.Errorf("reading shadow prelogin response: %w", err)
	}

	// Parse encryption option from the PRELOGIN response.
	var encryptionVal byte = preloginEncryptNotSup
	if len(preloginResp) > 0 && len(preloginResp[0].Payload) > 0 {
		encryptionVal = parsePreloginEncryption(preloginResp[0].Payload)
	}

	// If the server requires or defaults to encryption, upgrade to TLS.
	var loginConn net.Conn = shadowConn
	if encryptionVal == preloginEncryptOn || encryptionVal == preloginEncryptReq {
		tlsConn := tls.Client(shadowConn, &tls.Config{
			InsecureSkipVerify: true,
		})
		if err := tlsConn.Handshake(); err != nil {
			shadowConn.Close()
			return nil, fmt.Errorf("TLS handshake with shadow: %w", err)
		}
		loginConn = tlsConn
	}

	// 3. Send LOGIN7 over the (possibly TLS-wrapped) connection.
	login7Payload := buildLogin7Payload("sa", "Mori_P@ss1", dbName)
	login7Pkt := buildTDSPacket(typeLogin7, statusEOM, login7Payload)
	if _, err := loginConn.Write(login7Pkt); err != nil {
		shadowConn.Close()
		return nil, fmt.Errorf("sending shadow login7: %w", err)
	}

	// 4. Read login response.
	loginResp, err := readTDSMessage(loginConn)
	if err != nil {
		shadowConn.Close()
		return nil, fmt.Errorf("reading shadow login response: %w", err)
	}

	// Check for login errors.
	if len(loginResp) > 0 && len(loginResp[0].Payload) > 0 {
		if loginResp[0].Payload[0] == tokenError {
			shadowConn.Close()
			return nil, fmt.Errorf("shadow login failed")
		}
	}

	return loginConn, nil
}

// buildPreloginPayload constructs a PRELOGIN token stream with VERSION and ENCRYPTION options.
func buildPreloginPayload() []byte {
	// PRELOGIN is a set of option tokens. Each option: type(1) + offset(2 BE) + length(2 BE).
	// Terminated by 0xFF.

	// Option headers: VERSION(5 bytes) + ENCRYPTION(5 bytes) + terminator(1 byte) = 11 bytes
	dataOffset := 11

	var payload []byte

	// VERSION option: type=0x00, offset=dataOffset, length=6.
	payload = append(payload, 0x00)                                    // type: VERSION
	payload = append(payload, byte(dataOffset>>8), byte(dataOffset))   // offset (2 bytes BE)
	payload = append(payload, 0x00, 0x06)                              // length (6 bytes)

	// ENCRYPTION option: type=0x01, offset=dataOffset+6, length=1.
	encOffset := dataOffset + 6
	payload = append(payload, 0x01)                                    // type: ENCRYPTION
	payload = append(payload, byte(encOffset>>8), byte(encOffset))     // offset (2 bytes BE)
	payload = append(payload, 0x00, 0x01)                              // length (1 byte)

	// Terminator.
	payload = append(payload, 0xFF)

	// VERSION data: UL_VERSION(4) + US_SUBBUILD(2).
	payload = append(payload, 0x0F, 0x00, 0x10, 0x46) // Version bytes
	payload = append(payload, 0x00, 0x00)               // Sub-build

	// ENCRYPTION data: ENCRYPT_NOT_SUP (we prefer no encryption).
	payload = append(payload, preloginEncryptNotSup)

	return payload
}

// parsePreloginEncryption parses a PRELOGIN response payload and returns the ENCRYPTION option value.
// Returns preloginEncryptNotSup if the ENCRYPTION option is not found.
func parsePreloginEncryption(payload []byte) byte {
	i := 0
	for i < len(payload) {
		optType := payload[i]
		if optType == 0xFF {
			break
		}
		if i+5 > len(payload) {
			break
		}
		optOffset := int(payload[i+1])<<8 | int(payload[i+2])
		optLen := int(payload[i+3])<<8 | int(payload[i+4])
		if optType == 0x01 && optLen >= 1 && optOffset < len(payload) {
			return payload[optOffset]
		}
		i += 5
	}
	return preloginEncryptNotSup
}

// setPreloginEncryption finds the ENCRYPTION option in a PRELOGIN payload and
// sets it to the given value. No-ops if the option is not found.
func setPreloginEncryption(payload []byte, val byte) {
	setPreloginOption(payload, 0x01, val)
}

// setPreloginOption finds the given option type in a PRELOGIN payload and
// sets the first byte of its data to val. No-ops if the option is not found.
func setPreloginOption(payload []byte, optionType byte, val byte) {
	i := 0
	for i < len(payload) {
		optType := payload[i]
		if optType == 0xFF {
			break
		}
		if i+5 > len(payload) {
			break
		}
		optOffset := int(payload[i+1])<<8 | int(payload[i+2])
		optLen := int(payload[i+3])<<8 | int(payload[i+4])
		if optType == optionType && optLen >= 1 && optOffset < len(payload) {
			payload[optOffset] = val
			return
		}
		i += 5
	}
}

// buildLogin7Payload constructs a LOGIN7 packet payload.
func buildLogin7Payload(user, password, database string) []byte {
	// LOGIN7 has a fixed-size header (94 bytes for TDS 7.4) followed by variable-length data.

	userUTF16 := encodeUTF16LE(user)
	passUTF16 := encodeLogin7Password(password)
	dbUTF16 := encodeUTF16LE(database)
	appUTF16 := encodeUTF16LE("mori")
	serverUTF16 := encodeUTF16LE("")
	libraryUTF16 := encodeUTF16LE("go-mssqldb")

	// Fixed header size for LOGIN7.
	const fixedHeaderSize = 94

	// Calculate offsets for variable-length fields.
	dataStart := fixedHeaderSize

	// Offsets and lengths (in chars = bytes/2 for UTF-16).
	hostNameOffset := dataStart
	hostNameLen := 0

	userNameOffset := hostNameOffset + hostNameLen*2
	userNameLen := len(userUTF16) / 2

	passwordOffset := userNameOffset + userNameLen*2
	passwordLen := len(passUTF16) / 2

	appNameOffset := passwordOffset + passwordLen*2
	appNameLen := len(appUTF16) / 2

	serverNameOffset := appNameOffset + appNameLen*2
	serverNameLen := len(serverUTF16) / 2

	libraryOffset := serverNameOffset + serverNameLen*2
	libraryLen := len(libraryUTF16) / 2

	dbNameOffset := libraryOffset + libraryLen*2
	dbNameLen := len(dbUTF16) / 2

	totalLen := dbNameOffset + dbNameLen*2

	payload := make([]byte, totalLen)

	// Total length (4 bytes LE).
	putUint32LE(payload[0:4], uint32(totalLen))

	// TDS version (4 bytes): TDS 7.4 = 0x74000004.
	payload[4] = 0x04
	payload[5] = 0x00
	payload[6] = 0x00
	payload[7] = 0x74

	// Packet size (4 bytes LE): 4096.
	putUint32LE(payload[8:12], 4096)

	// Client program version (4 bytes).
	putUint32LE(payload[12:16], 0x07000000)

	// Client PID (4 bytes).
	putUint32LE(payload[16:20], 1234)

	// Connection ID (4 bytes).
	putUint32LE(payload[20:24], 0)

	// Option flags 1 (1 byte).
	payload[24] = 0xE0 // USE_DB_ON, INIT_DB_FATAL, SET_LANG_ON

	// Option flags 2 (1 byte).
	payload[25] = 0x03 // ODBC, INIT_LANG_FATAL

	// Type flags (1 byte).
	payload[26] = 0x00

	// Option flags 3 (1 byte).
	payload[27] = 0x00

	// Client timezone (4 bytes LE).
	putUint32LE(payload[28:32], 0)

	// Client LCID (4 bytes LE).
	putUint32LE(payload[32:36], 0x09040000) // en-US

	// HostName: offset(2 LE) + length(2 LE).
	putUint16LE(payload[36:38], uint16(hostNameOffset))
	putUint16LE(payload[38:40], uint16(hostNameLen))

	// UserName: offset(2 LE) + length(2 LE).
	putUint16LE(payload[40:42], uint16(userNameOffset))
	putUint16LE(payload[42:44], uint16(userNameLen))

	// Password: offset(2 LE) + length(2 LE).
	putUint16LE(payload[44:46], uint16(passwordOffset))
	putUint16LE(payload[46:48], uint16(passwordLen))

	// AppName: offset(2 LE) + length(2 LE).
	putUint16LE(payload[48:50], uint16(appNameOffset))
	putUint16LE(payload[50:52], uint16(appNameLen))

	// ServerName: offset(2 LE) + length(2 LE).
	putUint16LE(payload[52:54], uint16(serverNameOffset))
	putUint16LE(payload[54:56], uint16(serverNameLen))

	// Unused/Extension: offset(2 LE) + length(2 LE) = 0,0.
	putUint16LE(payload[56:58], 0)
	putUint16LE(payload[58:60], 0)

	// CltIntName (library): offset(2 LE) + length(2 LE).
	putUint16LE(payload[60:62], uint16(libraryOffset))
	putUint16LE(payload[62:64], uint16(libraryLen))

	// Language: offset(2 LE) + length(2 LE) = 0,0.
	putUint16LE(payload[64:66], 0)
	putUint16LE(payload[66:68], 0)

	// Database: offset(2 LE) + length(2 LE).
	putUint16LE(payload[68:70], uint16(dbNameOffset))
	putUint16LE(payload[70:72], uint16(dbNameLen))

	// ClientID (6 bytes) — MAC address, zeros is fine.
	// Already zero.

	// SSPI: offset(2 LE) + length(2 LE) = 0,0.
	putUint16LE(payload[78:80], 0)
	putUint16LE(payload[80:82], 0)

	// AtchDBFile: offset(2 LE) + length(2 LE) = 0,0.
	putUint16LE(payload[82:84], 0)
	putUint16LE(payload[84:86], 0)

	// ChangePassword: offset(2 LE) + length(2 LE) = 0,0.
	putUint16LE(payload[86:88], 0)
	putUint16LE(payload[88:90], 0)

	// SSPI Long (4 bytes LE) = 0.
	putUint32LE(payload[90:94], 0)

	// Variable-length data.
	// No hostname data (length 0).

	// UserName data.
	copy(payload[userNameOffset:], userUTF16)

	// Password data (already encoded).
	copy(payload[passwordOffset:], passUTF16)

	// AppName data.
	copy(payload[appNameOffset:], appUTF16)

	// ServerName data.
	copy(payload[serverNameOffset:], serverUTF16)

	// Library data.
	copy(payload[libraryOffset:], libraryUTF16)

	// Database data.
	copy(payload[dbNameOffset:], dbUTF16)

	return payload
}

// encodeLogin7Password encodes a password for the LOGIN7 packet.
// TDS LOGIN7 password encoding: each byte has its high and low nibbles swapped,
// then XORed with 0xA5.
func encodeLogin7Password(password string) []byte {
	utf16 := encodeUTF16LE(password)
	encoded := make([]byte, len(utf16))
	for i, b := range utf16 {
		// Swap nibbles and XOR with 0xA5.
		encoded[i] = ((b << 4) | (b >> 4)) ^ 0xA5
	}
	return encoded
}

func putUint16LE(b []byte, v uint16) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
}

func putUint32LE(b []byte, v uint32) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
}

// readRawBytes reads exactly n bytes from the reader.
func readRawBytes(r io.Reader, n int) ([]byte, error) {
	buf := make([]byte, n)
	_, err := io.ReadFull(r, buf)
	return buf, err
}
