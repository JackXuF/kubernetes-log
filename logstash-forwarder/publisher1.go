package main

import (
	"bytes"
	//"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Support for newer SSL signature algorithms
import _ "crypto/sha256"
import _ "crypto/sha512"

var hostname string
var hostport_re, _ = regexp.Compile("^(.+):([0-9]+)$")

func init() {
	hostname, _ = os.Hostname()
	rand.Seed(time.Now().UnixNano())
}

func Publishv1(input chan []*FileEvent,
	registrar chan []*FileEvent,
	config *NetworkConfig) {
	var buffer bytes.Buffer
	var socket net.Conn
	var sequence uint32
	var err error

	socket = connect(config)
	defer socket.Close()

	for events := range input {
		buffer.Truncate(0)
		//compressor, _ := zlib.NewWriterLevel(&buffer, 3)
		for _, event := range events {
			sequence += 1
			writeDataFrame(event, sequence, &buffer)
			//fmt.Println("Source: " + *event.Source)
		}
		//writeDataFrame(events[sequence-1], sequence, &buffer)
		//compressor.Flush()
		//compressor.Close()
		//writeDataFrame(event, sequence, &buffer)
		compressed_payload := buffer.Bytes()

		// Send buffer until we're successful...
		oops := func(err error) {
			// TODO(sissel): Track how frequently we timeout and reconnect. If we're
			// timing out too frequently, there's really no point in timing out since
			// basically everything is slow or down. We'll want to ratchet up the
			// timeout value slowly until things improve, then ratchet it down once
			// things seem healthy.
			emit("Socket error, will reconnect: %s\n", err)
			time.Sleep(1 * time.Second)
			socket.Close()
			socket = connect(config)
		}

		//SendPayload:
		for {
			// Abort if our whole request takes longer than the configured
			// network timeout.
			//socket.SetDeadline(time.Now().Add(config.timeout))

			// Set the window size to the length of this payload in events.
			/*_, err = socket.Write([]byte("1W"))
			if err != nil {
				oops(err)
				continue
			}
			binary.Write(socket, binary.BigEndian, uint32(len(events)))
			if err != nil {
				oops(err)
				continue
			}

			// Write compressed frame
			socket.Write([]byte("1C"))
			if err != nil {
				oops(err)
				continue
			}
			binary.Write(socket, binary.BigEndian, uint32(len(compressed_payload)))
			if err != nil {
				oops(err)
				continue
			}*/
			_, err = socket.Write(compressed_payload)
			if err != nil {
				oops(err)
				continue
			}

			// read ack
			/*
				response := make([]byte, 0, 6)
				ackbytes := 0
				for ackbytes != 6 {
					n, err := socket.Read(response[len(response):cap(response)])
					if err != nil {
						emit("Read error looking for ack: %s\n", err)
						socket.Close()
						socket = connect(config)
						continue SendPayload // retry sending on new connection
					} else {
						ackbytes += n
					}
				}*/

			// TODO(sissel): verify ack
			// Success, stop trying to send the payload.
			break
		}

		// Tell the registrar that we've successfully sent these events
		registrar <- events
	} /* for each event payload */
} // Publish

func connect(config *NetworkConfig) (socket net.Conn) {

	for {
		// Pick a random server from the list.
		hostport := config.Servers[rand.Int()%len(config.Servers)]
		submatch := hostport_re.FindSubmatch([]byte(hostport))
		if submatch == nil {
			fault("Invalid host:port given: %s", hostport)
		}
		host := string(submatch[1])
		port := string(submatch[2])
		addresses, err := net.LookupHost(host)

		if err != nil {
			emit("DNS lookup failure \"%s\": %s\n", host, err)
			time.Sleep(1 * time.Second)
			continue
		}

		address := addresses[rand.Int()%len(addresses)]
		var addressport string

		ip := net.ParseIP(address)
		if len(ip) == net.IPv4len {
			addressport = fmt.Sprintf("%s:%s", address, port)
		} else if len(ip) == net.IPv6len {
			addressport = fmt.Sprintf("[%s]:%s", address, port)
		}

		emit("Connecting to %s (%s) \n", addressport, host)

		socket, err = net.DialTimeout("tcp", addressport, config.timeout)
		if err != nil {
			emit("Failure connecting to %s: %s\n", address, err)
			time.Sleep(1 * time.Second)
			continue
		}
		//		socket.SetDeadline(time.Now().Add(config.timeout))
		emit("Connected to %s\n", address)
		//		socket.Write([]byte("that's what I do"))
		// connected, let's rock and roll.
		return
	}
	return
}

func writeDataFrame(event *FileEvent, sequence uint32, output io.Writer) {
	//emit("event: %s\n", *event.Text)
	// header, "1D"
	// sequence number
	/*binary.Write(output, binary.BigEndian, uint32(sequence))
	// 'pair' count
	binary.Write(output, binary.BigEndian, uint32(len(*event.Fields)+4))

	writeKV("file", *event.Source, output)
	writeKV("host", hostname, output)
	writeKV("offset", strconv.FormatInt(event.Offset, 10), output)
	writeKV("line", *event.Text, output)
	for k, v := range *event.Fields {
		writeKV(k, v, output)
	}*/
	_, file := filepath.Split(*event.Source)
	path := strings.Split(file, "-")[0]
	lent := strconv.Itoa(len(path + " " + *event.Text))
	content := lent + " " + path + " " + *event.Text
	output.Write([]byte(content))
}

func writeKV(key string, value string, output io.Writer) {
	//emit("kv: %d/%s %d/%s\n", len(key), key, len(value), value)
	binary.Write(output, binary.BigEndian, uint32(len(key)))
	output.Write([]byte(key))
	binary.Write(output, binary.BigEndian, uint32(len(value)))
	output.Write([]byte(value))
}
