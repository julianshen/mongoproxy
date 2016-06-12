package mongoproxy

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"
)

type LogMsg struct {
	Time     time.Time
	TimeUsed time.Duration
	Type     string
	Content  interface{}
}

func timed(f func()) time.Duration {
	t1 := time.Now()
	f()
	t2 := time.Now()
	return t2.Sub(t1)
}

func toJSON(v interface{}) string {
	b, _ := json.MarshalIndent(v, "", "    ")

	return string(b)
}

type Proxy struct {
	Remote  string
	Port    int
	LogResp bool
}

func (p *Proxy) Start() error {
	port := fmt.Sprintf(":%d", p.Port)
	listener, err := net.Listen("tcp", port)

	if err != nil {
		return err
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		go p.handleConnection(conn)
	}
}

func (p *Proxy) handleConnection(conn net.Conn) {
	c, e := net.Dial("tcp", p.Remote)
	defer c.Close()
	defer conn.Close()

	if e != nil {
		t := time.Now()
		l := LogMsg{Time: t, TimeUsed: 0, Type: "ERROR", Content: e}
		fmt.Println(toJSON(l))
		return
	}

	for {
		var r RequestMsg
		var e error

		t := time.Now()
		d := timed(func() {
			r, e = ReadRequest(conn)
		})

		if e != nil {
			if e != io.EOF {
				l := LogMsg{Time: t, TimeUsed: d, Type: "ERROR", Content: e}
				fmt.Println(toJSON(l))
			}
			break
		}

		l := LogMsg{Time: t, TimeUsed: d, Type: r.GetOp().String(), Content: &r}

		fmt.Println(toJSON(l))

		d = timed(func() {
			e = WriteRequest(r, c)

			if e != nil {
				return
			}

			r, e = ReadRequest(c) //Read reply
		})

		if e != nil {
			if e != io.EOF {
				l := LogMsg{Time: t, TimeUsed: d, Type: "ERROR", Content: e}
				fmt.Println(toJSON(l))
			}
			break
		} else {
			if p.LogResp {
				l := LogMsg{Time: t, TimeUsed: d, Type: r.GetOp().String(), Content: &r}
				fmt.Println(toJSON(l))
			}
			WriteRequest(r, conn)
		}
	}
}
