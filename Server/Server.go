package main

import (
	"./models"
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Frame struct {
	Frames []string
}

var Contopath map[string]string

var frame_chan = make(chan Frame)

const logfile_maxsize = 5 * 1024 * 1024 // 5M
const deadLogCheckPeriod = time.Second * 5
const oldLogCheckCount = 4 //deadLogCheckPeriod 的 4 倍
const oldTimeout = -1      //days ago

var serveraddr string
var rcurl string
var podurl string

func init() {
	flag.StringVar(&serveraddr, "server", "", "master ip address")
}

func main() {
	log.SetFlags(log.Lshortfile)
	flag.Parse()

	if serveraddr == "" {
		print("server address should be given!")
		os.Exit(0)
	}

	rcurl = "http://" + serveraddr + ":8080/api/v1/namespaces/default/replicationcontrollers"
	podurl = "http://" + serveraddr + ":8080/api/v1/namespaces/default/pods"

	Contopath = make(map[string]string)
	initDir()

	for k, v := range Contopath {
		print(k + " " + v)
	}

	ln, err := net.Listen("tcp", ":3333")
	if err != nil {
		log.Println(err)
		return
	}
	defer ln.Close()
	defer close(frame_chan)

	go redirectLog()
	go clearLog()

	for {
		conn, err := ln.Accept()

		defer conn.Close()
		if err != nil {
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	channel := make(chan []byte, 4096)

	defer close(channel)
	go extract(channel)

	for {
		data := make([]byte, 4096)
		i, err := conn.Read(data)
		if err == nil {
			channel <- data[0:i]
		} else {
			break
		}
	}
}

func clearLog() {
	user, _ := user.Current()
	var dir = user.HomeDir + "/Applog"
	var count = 0
	var oldCheckActivated = false
	for {
		time.Sleep(deadLogCheckPeriod)
		count++
		if _, err := os.Stat(dir); err != nil {
			continue
		}

		//get all apps running in Kubenate
		var appnames []string
		resp, _ := http.Get(rcurl)
		var rclist models.ReplicationControllerList
		body, _ := ioutil.ReadAll(resp.Body)
		json.Unmarshal(body, &rclist)
		for _, v := range rclist.Items {
			appname := v.ObjectMeta.Labels["name"]
			appnames = append(appnames, appname)
		}

		//get all sub dirs(appnames),
		entries, err := ioutil.ReadDir(dir)
		if err != nil {
			continue
		}

		for _, subdirname := range entries {
			var found = false
			for _, appname := range appnames {
				if subdirname.Name() == appname {
					found = true
					break
				}
			}

			subdirpath := path.Join(dir, subdirname.Name())

			if found {
				entries, err := ioutil.ReadDir(subdirpath)
				if err != nil {
					continue
				}
				//remove logs
				ids := getAllIdByName(subdirname.Name())
				for _, container := range entries {
					var isDead = true
					for _, id := range ids {
						if id == container.Name() {
							isDead = false
							break
						}
					}
					//remove dead logs
					if isDead {
						if Contopath[container.Name()] != "" {
							delete(Contopath, container.Name())
						}
						os.RemoveAll(path.Join(subdirpath, container.Name()))
					}

					//remove old logs
					if count >= oldLogCheckCount {
						oldCheckActivated = true
						logfiles, err := ioutil.ReadDir(path.Join(subdirpath, container.Name()))
						if err != nil {
							continue
						}
						for _, logname := range logfiles {
							lname := logname.Name()
							if !strings.HasSuffix(lname, ".log") {
								continue
							}
							dateStamp := lname[len(lname)-12 : len(lname)-4]
							daysAgo := time.Now().AddDate(0, 0, oldTimeout).Format("20060102")
							if dateStamp < daysAgo {
								os.Remove(path.Join(subdirpath, container.Name(), lname))
							}
						}
					}
				}
			} else {
				entries, err := ioutil.ReadDir(subdirpath)
				if err != nil {
					continue
				}

				for _, cid := range entries {
					if Contopath[cid.Name()] != "" {
						delete(Contopath, cid.Name())
					}
				}
				os.RemoveAll(subdirpath)
			}
		}
		if oldCheckActivated {
			count = 0
			oldCheckActivated = false
		}
	}
}

func getAllIdByName(appname string) (ids []string) {
	rsp, _ := http.Get(podurl + "?labelSelector=name%3D" + appname)
	var podlist models.PodList
	body, _ := ioutil.ReadAll(rsp.Body)
	json.Unmarshal(body, &podlist)
	for _, p := range podlist.Items {
		for _, c := range p.Status.ContainerStatuses {
			if c.ContainerID == "" {
				continue
			}
			id := c.ContainerID[9:]
			ids = append(ids, id)
		}
	}
	return
}

func extract(channel chan []byte) {
	var buf bytes.Buffer
	for {
		temp := <-channel
		if len(temp) <= 0 {
			break
		}
		buf.Write(temp)

		var list []string
		for {
			length, err := buf.ReadString([]byte(string(" "))[0])

			//no more bytes is available, reset and write the length back
			if err == io.EOF {
				buf.Reset()
				buf.WriteString(length)
				break
			}
			actualLength, _ := strconv.Atoi(strings.TrimSpace(length))

			//if available bytes is less than what we wanted
			if actualLength > buf.Len() {
				remain := buf.Bytes()
				buf.Reset()
				buf.WriteString(length + string(remain))
				break
			}
			frame := make([]byte, actualLength)
			buf.Read(frame)
			list = append(list, string(frame))
		}
		frame := Frame{Frames: list}
		frame_chan <- frame
	}
}

func initDir() {
	user, _ := user.Current()
	Dir := user.HomeDir + "/Applog"
	_, err := os.Stat(Dir)
	if err != nil {
		os.Mkdir(Dir, 0777)
	}

	resp, _ := http.Get(rcurl)
	var rclist models.ReplicationControllerList
	body, _ := ioutil.ReadAll(resp.Body)
	json.Unmarshal(body, &rclist)
	for _, v := range rclist.Items {
		appname := v.ObjectMeta.Labels["name"]
		if appname == "apm" {
			continue
		}

		rsp, _ := http.Get(podurl + "?labelSelector=name%3D" + appname)

		var podlist models.PodList

		body, _ = ioutil.ReadAll(rsp.Body)
		json.Unmarshal(body, &podlist)

		localDir := Dir + "/" + appname
		_, err = os.Stat(localDir)
		if err != nil {
			os.Mkdir(localDir, 0777)
		}

		for _, p := range podlist.Items {
			for _, c := range p.Status.ContainerStatuses {
				if c.ContainerID == "" {
					continue
				}
				containerid := c.ContainerID[9:]

				containerpth := localDir + "/" + containerid
				_, err = os.Stat(containerpth)
				if err != nil {
					os.Mkdir(containerpth, 0777)
				}

				file := containerpth + "/" + containerid + "-" + strconv.Itoa(findNewest(containerpth)) + "-" + time.Now().Format("20060102") + ".log"
				os.Create(file)
				Contopath[containerid] = file
			}
		}
	}
}

func findNewest(containerpath string) (maxNumber int) {
	maxNumber = 1
	entries, err := ioutil.ReadDir(containerpath)
	if err != nil {
		return
	}
	current := time.Now().Format("20060102")
	for _, logname := range entries {
		lname := logname.Name()
		if !strings.HasSuffix(lname, ".log") {
			continue
		}
		date := lname[len(lname)-12 : len(lname)-4]
		if date == current {
			number, _ := strconv.Atoi(strings.Split(lname, "-")[1])
			if number > maxNumber {
				maxNumber = number
			}
		}
	}
	return
}

func scan(contain string) {
	user, _ := user.Current()
	Dir := user.HomeDir + "/Applog"

	resp, _ := http.Get(rcurl)
	var rclist models.ReplicationControllerList
	body, _ := ioutil.ReadAll(resp.Body)
	json.Unmarshal(body, &rclist)
	for _, v := range rclist.Items {
		appname := v.ObjectMeta.Labels["name"]
		if appname == "apm" {
			continue
		}

		rsp, _ := http.Get(podurl + "?labelSelector=name%3D" + appname)

		var podlist models.PodList

		body, _ = ioutil.ReadAll(rsp.Body)
		json.Unmarshal(body, &podlist)

		localDir := Dir + "/" + appname
		_, err := os.Stat(localDir)
		if err != nil {
			os.Mkdir(localDir, 0777)
		}

		for _, p := range podlist.Items {
			for _, c := range p.Status.ContainerStatuses {
				if c.ContainerID == "" {
					continue
				}
				containerid := c.ContainerID[9:]
				if contain == containerid {
					containerpth := localDir + "/" + containerid
					_, err = os.Stat(containerpth)
					if err != nil {
						os.Mkdir(containerpth, 0777)
					} else {
						return
					}
					file := containerpth + "/" + containerid + "-1-" + time.Now().Format("20060102") + ".log"
					_, err = os.Create(file)
					Contopath[containerid] = file
					return
				}
			}
		}
	}
	return
}

func redirectLog() {
	for {
		frame := <-frame_chan
		buffer := make(map[string]string)
		for _, f := range frame.Frames {
			index := strings.Index(f, " ")
			if index == -1 {
				continue
			}
			id := f[:index]
			log := f[index+1:]
			if log != "" {
				buffer[id] += log + "\n"
			}
		}
		writeLog(buffer)
	}
}

func writeLog(buffer map[string]string) {
	for k, v := range buffer {
		path := Contopath[k]
		if path == "" {
			scan(k)
			path = Contopath[k]
			if path == "" {
				continue
			}
		}
		fdate := path[len(path)-12 : len(path)-4]
		if fdate == time.Now().Format("20060102") {
			if fstat, err := os.Stat(path); err == nil && fstat.Size() > logfile_maxsize {
				dname, fname := filepath.Split(path)
				number, _ := strconv.Atoi(strings.Split(fname, "-")[1])
				newnumber := strconv.Itoa(number + 1)
				newfile := dname + k + "-" + newnumber + "-" + fdate + ".log"
				f, _ := os.Create(newfile)
				Contopath[k] = newfile
				io.WriteString(f, v)
				f.Close()
			} else if ff, err := os.OpenFile(path, os.O_APPEND|os.O_RDWR, 0666); err == nil {
				io.WriteString(ff, v)
				ff.Close()
			}
		} else {
			dname, _ := filepath.Split(path)
			file := dname + k + "-1-" + time.Now().Format("20060102") + ".log"
			f, _ := os.Create(file)
			io.WriteString(f, v)
			Contopath[k] = file
			f.Close()
		}

	}
}
