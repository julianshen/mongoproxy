package main

import (
	mp "github.com/julianshen/mongoproxy"
	flag "github.com/ogier/pflag"
)

var (
	dstHost           string
	port              int
	shouldLogResponse bool
)

func init() {
	flag.StringVar(&dstHost, "remote", "localhost:27017", "Remote host name and port of the Mongodb (default localhost:port)")
	flag.IntVar(&port, "port", 50001, "Local proxy port (default 50001)")
	flag.BoolVar(&shouldLogResponse, "response", false, "Log response")
	flag.Parse()
}

func main() {
	proxy := mp.Proxy{Remote: dstHost, Port: port, LogResp: shouldLogResponse}
	e := proxy.Start()

    if e != nil {
        panic(e)
    }
}
