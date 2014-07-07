package main

import (
    "os"
	"fmt"
	"net"
	"net/http"
	"log"
	"strings"
	"bytes"
	"encoding/xml"
	"io"
	"bufio"
	"runtime"
	"flag"
)

//flags
var (
    keyFile = flag.String("kfile", "/usr/share/barzer/auth_keys.txt", "Key file (key:uid)")
	barzerAddr = flag.String("bhost", "localhost:5767", "Barzer hostname:port")
	listenAddr = flag.String("listen", ":8090", "Listen on addr:port")
	verbose = flag.Bool("v", false, "Verbose output")
)

// types
type dict map[string]string
type MyHandler struct {}


// you saw nothing
var keyStorage dict = dict{}

// utility functions
func lookupUser(key string) (string, bool) {
	val, ok := keyStorage[key]
	return val, ok
}

// meh
func errorXML(w io.Writer, e string) {
	fmt.Fprintf(w, "<error>%s</error>", e)
}
// meh2
func errorJSON(w io.Writer, e string) {
	fmt.Fprintf(w, "{ \"error\": \"%s\" }", e)
}

// meh3
func genQuery(query string, args dict) []byte {
	var out bytes.Buffer
	out.WriteString("<query");
	for key, val := range args {
		fmt.Fprintf(&out, " %s=\"", key)
		xml.EscapeText(&out, []byte(val))
		out.WriteString("\"")

	}
	out.WriteString(">")
	xml.EscapeText(&out, []byte(query))
	out.WriteString("</query>\r\n.\r\n")
	
	return out.Bytes()
}

// -----------------------
func query(w http.ResponseWriter, req *http.Request, qtype string) {
	var (
		ctype string
		error_write func(io.Writer, string)
		query string
		//version float64
	)

	switch qtype {
	case "json", "sjson":
		ctype = "application/json"
		error_write = errorJSON
	case "xml":
		ctype = "text/xml"
		error_write = errorXML
	default:
		fmt.Fprintln(w, "Unknown query type")
		return
	}
	
	args := dict{ "ret": qtype }
	for key, val := range req.URL.Query() {
		if len(val) < 1 { continue }
		v := val[0]
		switch key {
		case "query", "q":
			query = v
		case "key":
			if uid, ok := lookupUser(v); ok {
			    args["u"] = uid
			}
		case "ver":
			//
		case "now","beni","zurch","flag","route",
			 "extra","u","uname","byid","zdtag":
			 args[key] = v
		}
	}
	w.Header().Set("Content-Type", ctype + "; charset=utf-8")
	if _, ok := args["u"]; !ok {
		error_write(w, "Unknown User")
		return
	}
	
	conn, err := net.Dial("tcp", *barzerAddr)
	if err != nil {
		error_write(w, "Error connecting to barzer")
		return
	}
	
	qbytes := genQuery(query, args)

	conn.Write(qbytes)
	if *verbose {
		log.Print(string(qbytes[:len(qbytes) - 5])) 
	}
	
	// this doesn't use sendfile(2) for some reason
	io.Copy(w, conn)
	conn.Close()
}


func (h *MyHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	parts := strings.Split(req.URL.Path[1:], "/")
	switch parts[0] {
	case "query":
		var qtype string
		if len(parts) > 1 && parts[1] != "" {
			qtype = parts[1]
		} else {
			qtype = "json"
		}
		query(w, req, qtype)
	default:
		fmt.Fprintln(w, "Unknown action")
	}
}


// give me fsnotify already
func loadKeys() {
    fh, err := os.Open(*keyFile)
    if err != nil {
        log.Println("Unable to open ", *keyFile)
        return
    }
    log.Println("Loading keys from ", *keyFile)
    defer fh.Close()
    r := bufio.NewReader(fh)
    
	var (
	    cnt int = 0
	    buf, line []byte
	)
    for err == nil {
 		var prefix bool = true

   		for prefix { 
        	line, prefix, err = r.ReadLine()
            buf = append(buf, line...)
        }
   		if len(buf) == 0 { break }
   		parts := strings.SplitN(string(buf), "|", 2)
   		if len(parts) < 2 { break }
   		if *verbose {
   	   		log.Printf("%s => %s\n", parts[0], parts[1])
   	    }
   		keyStorage[parts[0]] = parts[1]
		buf = buf[:0]
		cnt++
    }
    log.Println("Loaded", cnt, "users")
 }


func main() {
    flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())
	loadKeys()
	x := MyHandler{}
	log.Print("Serving...\n")
	err := http.ListenAndServe(*listenAddr, &x)
	if err != nil {
		log.Fatal("Unable to listen: ", err)
	}
}