package proxy

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// RESPValue represents a value in the Redis Serialization Protocol.
type RESPValue struct {
	Type   byte        // '+', '-', ':', '$', '*'
	Str    string      // for simple string, error, bulk string
	Int    int64       // for integer
	Array  []RESPValue // for array
	IsNull bool        // for null bulk string / null array
}

// ReadRESPValue reads a single RESP value from a buffered reader.
func ReadRESPValue(r *bufio.Reader) (*RESPValue, error) {
	typeByte, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	switch typeByte {
	case '+':
		return readSimpleString(r)
	case '-':
		return readError(r)
	case ':':
		return readInteger(r)
	case '$':
		return readBulkString(r)
	case '*':
		return readArray(r)
	default:
		// Might be an inline command (no RESP prefix).
		// Unread the byte and try to read a line as inline command.
		if err := r.UnreadByte(); err != nil {
			return nil, err
		}
		return readInlineCommand(r)
	}
}

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

func readSimpleString(r *bufio.Reader) (*RESPValue, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	return &RESPValue{Type: '+', Str: line}, nil
}

func readError(r *bufio.Reader) (*RESPValue, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	return &RESPValue{Type: '-', Str: line}, nil
}

func readInteger(r *bufio.Reader) (*RESPValue, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	n, err := strconv.ParseInt(line, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid RESP integer: %q", line)
	}
	return &RESPValue{Type: ':', Int: n}, nil
}

func readBulkString(r *bufio.Reader) (*RESPValue, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	length, err := strconv.Atoi(line)
	if err != nil {
		return nil, fmt.Errorf("invalid bulk string length: %q", line)
	}
	if length < 0 {
		return &RESPValue{Type: '$', IsNull: true}, nil
	}
	buf := make([]byte, length+2) // +2 for \r\n
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("reading bulk string data: %w", err)
	}
	return &RESPValue{Type: '$', Str: string(buf[:length])}, nil
}

func readArray(r *bufio.Reader) (*RESPValue, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	count, err := strconv.Atoi(line)
	if err != nil {
		return nil, fmt.Errorf("invalid array count: %q", line)
	}
	if count < 0 {
		return &RESPValue{Type: '*', IsNull: true}, nil
	}
	arr := make([]RESPValue, count)
	for i := 0; i < count; i++ {
		elem, err := ReadRESPValue(r)
		if err != nil {
			return nil, fmt.Errorf("reading array element %d: %w", i, err)
		}
		arr[i] = *elem
	}
	return &RESPValue{Type: '*', Array: arr}, nil
}

func readInlineCommand(r *bufio.Reader) (*RESPValue, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	line = strings.TrimSpace(line)
	if line == "" {
		return &RESPValue{Type: '*', Array: nil}, nil
	}
	parts := strings.Fields(line)
	arr := make([]RESPValue, len(parts))
	for i, part := range parts {
		arr[i] = RESPValue{Type: '$', Str: part}
	}
	return &RESPValue{Type: '*', Array: arr}, nil
}

// WriteRESPValue writes a RESP value to a writer.
func WriteRESPValue(w io.Writer, v *RESPValue) error {
	_, err := w.Write(v.Bytes())
	return err
}

// Bytes serializes a RESP value to raw bytes.
func (v *RESPValue) Bytes() []byte {
	var b []byte
	switch v.Type {
	case '+':
		b = append(b, '+')
		b = append(b, v.Str...)
		b = append(b, '\r', '\n')
	case '-':
		b = append(b, '-')
		b = append(b, v.Str...)
		b = append(b, '\r', '\n')
	case ':':
		b = append(b, ':')
		b = append(b, strconv.FormatInt(v.Int, 10)...)
		b = append(b, '\r', '\n')
	case '$':
		if v.IsNull {
			b = append(b, '$', '-', '1', '\r', '\n')
		} else {
			b = append(b, '$')
			b = append(b, strconv.Itoa(len(v.Str))...)
			b = append(b, '\r', '\n')
			b = append(b, v.Str...)
			b = append(b, '\r', '\n')
		}
	case '*':
		if v.IsNull {
			b = append(b, '*', '-', '1', '\r', '\n')
		} else {
			b = append(b, '*')
			b = append(b, strconv.Itoa(len(v.Array))...)
			b = append(b, '\r', '\n')
			for _, elem := range v.Array {
				b = append(b, elem.Bytes()...)
			}
		}
	}
	return b
}

// ParseCommand extracts the command name and arguments from a RESP array value.
func ParseCommand(v *RESPValue) (cmd string, args []string, err error) {
	if v.Type != '*' || v.IsNull || len(v.Array) == 0 {
		return "", nil, fmt.Errorf("expected RESP array, got type %c", v.Type)
	}
	cmd = strings.ToUpper(v.Array[0].Str)
	for i := 1; i < len(v.Array); i++ {
		args = append(args, v.Array[i].Str)
	}
	return cmd, args, nil
}

// BuildCommandArray builds a RESP array for a Redis command.
func BuildCommandArray(args ...string) *RESPValue {
	arr := make([]RESPValue, len(args))
	for i, arg := range args {
		arr[i] = RESPValue{Type: '$', Str: arg}
	}
	return &RESPValue{Type: '*', Array: arr}
}

// BuildErrorReply builds a RESP error value.
func BuildErrorReply(msg string) *RESPValue {
	return &RESPValue{Type: '-', Str: msg}
}

// BuildSimpleString builds a RESP simple string value.
func BuildSimpleString(s string) *RESPValue {
	return &RESPValue{Type: '+', Str: s}
}

// BuildBulkString builds a RESP bulk string value.
func BuildBulkString(s string) *RESPValue {
	return &RESPValue{Type: '$', Str: s}
}

// BuildNullBulkString builds a RESP null bulk string.
func BuildNullBulkString() *RESPValue {
	return &RESPValue{Type: '$', IsNull: true}
}

// BuildInteger builds a RESP integer value.
func BuildInteger(n int64) *RESPValue {
	return &RESPValue{Type: ':', Int: n}
}

// CommandToInline converts a RESP array command to inline string form.
// Used for classifier input: "SET user:1 value".
func CommandToInline(v *RESPValue) string {
	if v.Type != '*' || v.IsNull || len(v.Array) == 0 {
		return ""
	}
	parts := make([]string, len(v.Array))
	for i, elem := range v.Array {
		parts[i] = elem.Str
	}
	return strings.Join(parts, " ")
}
