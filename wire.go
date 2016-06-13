package mongoproxy

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"gopkg.in/mgo.v2/bson"
	"io"
	"reflect"
)

type Opcode int32

const (
	OP_REPLY = Opcode(1)
	OP_MSG   = Opcode(1000)
)

const (
	_ = iota + 2000
	OP_UPDATE
	OP_INSERT
	RESERVED
	OP_QUERY
	OP_GET_MORE
	OP_DELETE
	OP_KILL_CURSORS
)

const (
	HEADER_SIZE = 16
)

//ERRORS
var (
	ErrorWrongLen      error = errors.New("Wrong data length")
	ErrorOpcodeUnknown error = errors.New("OP Code unknown")
)

type MsgHeader struct {
	MessageLength int32 // total message size, including this
	RequestID     int32 // identifier for this message
	ResponseTo    int32 // requestID from the original request
	//   (used in responses from db)
	Opcode // request type - see table below
}

func readMsgHeader(r io.Reader) (*MsgHeader, error) {
	h := MsgHeader{}
	err := binary.Read(r, binary.LittleEndian, &h)

	if err != nil {
		return nil, err
	}
	return &h, nil
}

func readInt32s(r io.Reader, n int) ([]int32, error) {
	v := make([]int32, n)

	for i := 0; i < n; i++ {
		var val int32
		err := binary.Read(r, binary.LittleEndian, &val)

		if err != nil {
			return nil, err
		}

		v[i] = val
	}

	return v, nil
}

func readInt64s(r io.Reader, n int) ([]int64, error) {
	v := make([]int64, n)

	for i := 0; i < n; i++ {
		var val int64
		err := binary.Read(r, binary.LittleEndian, &val)

		if err != nil {
			return nil, err
		}

		v[i] = val
	}

	return v, nil
}

func readDoc(r *bufio.Reader) (bson.D, int, error) {
	b, e := r.Peek(4)
	if e != nil {
		return nil, 0, e
	}

	var n int32
	binary.Read(bytes.NewReader(b), binary.LittleEndian, &n)

	bDoc := make([]byte, n)
	_, e = io.ReadFull(r, bDoc)

	if e != nil {
		return nil, 0, e
	}

	var out bson.D
	e = bson.Unmarshal(bDoc, &out)

	if e != nil {
		return nil, 0, e
	}

	return out, int(n), nil
}

func writeBson(data bson.D, w *bufio.Writer) error {
	if len(data) > 0 {
		bytes, err := bson.Marshal(data)
		if err != nil {
			return err
		}
		_, e := w.Write(bytes)
		return e
	}
	return nil
}

func (o Opcode) String() string {
	switch o {
	case OP_DELETE:
		return "OP_DELETE"
	case OP_GET_MORE:
		return "OP_GET_MORE"
	case OP_INSERT:
		return "OP_INSERT"
	case OP_KILL_CURSORS:
		return "OP_KILL_CURSORS"
	case OP_MSG:
		return "OP_MSG"
	case OP_QUERY:
		return "OP_QUERY"
	case OP_REPLY:
		return "OP_REPLY"
	case OP_UPDATE:
		return "OP_UPDATE"
	}

	return "UNKNOWN"
}

type RequestMsg interface {
	GetOp() Opcode
}

type Query struct {
	*MsgHeader
	Flags              int32  // bit vector of query options.  See below for details.
	FullCollectionName string // "dbname.collectionname"
	NumberToSkip       int32  // number of documents to skip
	NumberToReturn     int32  // number of documents to return
	//  in the first OP_REPLY batch
	Query                bson.D // query object.  See below for details.
	ReturnFieldsSelector bson.D // Optional. Selector indicating the fields
	//  to return.  See below for details.
}

func (req *Query) GetOp() Opcode {
	return OP_QUERY
}

type Update struct {
	*MsgHeader                // standard message header
	ZERO               int32  // 0 - reserved for future use
	FullCollectionName string // "dbname.collectionname"
	Flags              int32  // bit vector. see below
	Selector           bson.D // the query to select the document
	Update             bson.D // specification of the update to perform
}

func (req *Update) GetOp() Opcode {
	return OP_UPDATE
}

type Insert struct {
	*MsgHeader                  // standard message header
	Flags              int32    // bit vector - see below
	FullCollectionName string   // "dbname.collectionname"
	Documents          []bson.D // one or more documents to insert into the collection
}

func (req *Insert) GetOp() Opcode {
	return OP_INSERT
}

type GetMore struct {
	*MsgHeader                // standard message header
	ZERO               int32  // 0 - reserved for future use
	FullCollectionName string // "dbname.collectionname"
	NumberToReturn     int32  // number of documents to return
	CursorID           int64  // cursorID from the OP_REPLY
}

func (req *GetMore) GetOp() Opcode {
	return OP_GET_MORE
}

type Delete struct {
	*MsgHeader                // standard message header
	ZERO               int32  // 0 - reserved for future use
	FullCollectionName string // "dbname.collectionname"
	Flags              int32  // bit vector - see below for details.
	Selector           bson.D // query object.  See below for details.
}

func (req *Delete) GetOp() Opcode {
	return OP_DELETE
}

type KillCursors struct {
	*MsgHeader                // standard message header
	ZERO              int32   // 0 - reserved for future use
	NumberOfCursorIDs int32   // number of cursorIDs in message
	CursorIDs         []int64 // sequence of cursorIDs to close
}

func (req *KillCursors) GetOp() Opcode {
	return OP_KILL_CURSORS
}

type Msg struct {
	*MsgHeader        // standard message header
	Message    string // message for the database
}

func (req *Msg) GetOp() Opcode {
	return OP_MSG
}

type Reply struct {
	*MsgHeader              // standard message header
	ResponseFlags  int32    // bit vector - see details below
	CursorID       int64    // cursor id if client needs to do get more's
	StartingFrom   int32    // where in the cursor this reply is starting
	NumberReturned int32    // number of documents in the reply
	Documents      []bson.D // documents
}

func (req *Reply) GetOp() Opcode {
	return OP_REPLY
}

func newReq(h *MsgHeader) RequestMsg {
	var ret RequestMsg
	switch h.Opcode {
	case OP_UPDATE:
		ret = &Update{MsgHeader: h}
	case OP_QUERY:
		ret = &Query{MsgHeader: h}
	case OP_DELETE:
		ret = &Delete{MsgHeader: h}
	case OP_GET_MORE:
		ret = &GetMore{MsgHeader: h}
	case OP_INSERT:
		ret = &Insert{MsgHeader: h}
	case OP_KILL_CURSORS:
		ret = &KillCursors{MsgHeader: h}
	case OP_MSG:
		ret = &Msg{MsgHeader: h}
	case OP_REPLY:
		ret = &Reply{MsgHeader: h}
	}
	return ret
}

func ReadRequest(r io.Reader) (RequestMsg, error) {
	h, e := readMsgHeader(r)

	if e != nil {
		return nil, e
	}

	req := newReq(h)
	if req == nil {
		return nil, ErrorOpcodeUnknown
	}

	bytesRead := HEADER_SIZE
	bufferReader := bufio.NewReader(r)

	v := reflect.ValueOf(req)
	v = v.Elem()

	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		t := f.Type()

		if bytesRead == int(h.MessageLength) {
			break
		} else if bytesRead > int(h.MessageLength) {
			return nil, ErrorWrongLen
		}

		switch {
		case t == reflect.TypeOf((bson.D)(nil)):
			d, n, e := readDoc(bufferReader)
			if e != nil {
				return nil, e
			}

			f.Set(reflect.ValueOf(d))
			bytesRead += n

		case t == reflect.TypeOf(([]bson.D)(nil)):
			var data []bson.D
			for {
				d, n, e := readDoc(bufferReader)
				if e != nil {
					return nil, e
				}

				data = append(data, d)
				bytesRead += n

				if bytesRead == int(h.MessageLength) {
					break
				}
			}
			f.Set(reflect.ValueOf(data))
		case t == reflect.TypeOf(([]int64)(nil)):
			var data []int64
			for {
				n, e := readInt64s(bufferReader, 1)

				if e != nil {
					return nil, e
				}

				data = append(data, n...)
				bytesRead += 8
				if bytesRead == int(h.MessageLength) {
					break
				}
			}
			f.Set(reflect.ValueOf(data))
		case t.Kind() == reflect.String:
			str, e := bufferReader.ReadString(byte(0))
			if e != nil {
				return nil, e
			}
			f.SetString(str[:len(str)-1]) //Exclude \x00
			bytesRead += len(str)
		case t.Kind() == reflect.Int32:
			n, e := readInt32s(bufferReader, 1)

			if e != nil {
				return nil, e
			}
			f.Set(reflect.ValueOf(n[0]))
			bytesRead += 4
		case t.Kind() == reflect.Int64:
			n, e := readInt64s(bufferReader, 1)

			if e != nil {
				return nil, e
			}
			f.Set(reflect.ValueOf(n[0]))
			bytesRead += 8
		default:
			//Skip
		}
	}

	return req, nil
}

func WriteRequest(req RequestMsg, w io.Writer) error {
	bufWriter := bufio.NewWriter(w)
	defer bufWriter.Flush()

	v := reflect.ValueOf(req)
	v = v.Elem()

	var e error
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		t := f.Type()
		switch {
		case t == reflect.TypeOf((*MsgHeader)(nil)):
			e = binary.Write(bufWriter, binary.LittleEndian, f.Elem().Interface())
		case t.Kind() == reflect.Int32, t.Kind() == reflect.Int64:
			e = binary.Write(bufWriter, binary.LittleEndian, f.Interface())
		case t.Kind() == reflect.String:
			_, e = bufWriter.WriteString(f.String())
			bufWriter.WriteByte(0) //Terminate with \x00
		case t == reflect.TypeOf((bson.D)(nil)):
			e = writeBson(f.Interface().(bson.D), bufWriter)
		case t == reflect.TypeOf(([]bson.D)(nil)):
			data := f.Interface().([]bson.D)

			for _, d := range data {
				e = writeBson(d, bufWriter)

				if e != nil {
					break
				}
			}
		case t == reflect.TypeOf(([]int64)(nil)):
			data := f.Interface().([]int64)
			for _, d := range data {
				e = binary.Write(bufWriter, binary.LittleEndian, d)

				if e != nil {
					break
				}
			}
		}
	}

	if e != nil {
		return e
	}

	return nil
}
