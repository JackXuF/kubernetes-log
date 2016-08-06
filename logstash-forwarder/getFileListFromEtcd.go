package main

import (
	"./models"
	"encoding/json"
	"github.com/coreos/go-etcd/etcd"
	"os"
	"time"
)

const prefix = "/var/lib/docker/containers/"
const suffix = "-json.log"
const key = "/registry/pods/default"
const logfile_maxsize = 50 * 1024 * 1024
const getPeriod = time.Second * 2
const clearPeriod = time.Minute * 30

type Pick struct {
	Filelist []string
}

func GetFileList(flist chan Pick, server string) {
	addr := "http://" + server + ":4001"
	machines := []string{addr}
	client := etcd.NewClient(machines)

	for {
		rsp, _ := client.Get(key, false, true)
		var list []string
		for _, node := range rsp.Node.Nodes {
			var pod models.Pod
			json.Unmarshal([]byte(node.Value), &pod)
			if pod.ObjectMeta.Labels["name"] != "" && pod.ObjectMeta.Labels["name"] != "apm" {
				for _, cstatus := range pod.Status.ContainerStatuses {
					if cstatus.ContainerID == "" {
						continue
					}
					id := cstatus.ContainerID[9:]
					filename := prefix + id + "/" + id + suffix
					list = append(list, filename)
				}
			}
		}
		pick := Pick{Filelist: list}
		flist <- pick
		// Defer next scan for a bit.
		time.Sleep(getPeriod) // Make this tunable
	}
}

func clearLog(server string) {
	addr := "http://" + server + ":4001"
	machines := []string{addr}
	client := etcd.NewClient(machines)
	for {
		rsp, _ := client.Get(key, false, true)
		for _, node := range rsp.Node.Nodes {
			var pod models.Pod
			json.Unmarshal([]byte(node.Value), &pod)
			if pod.ObjectMeta.Labels["name"] != "" && pod.ObjectMeta.Labels["name"] != "apm" {
				for _, cstatus := range pod.Status.ContainerStatuses {
					if cstatus.ContainerID == "" {
						continue
					}
					id := cstatus.ContainerID[9:]
					filename := prefix + id + "/" + id + suffix

					if fstat, erro := os.Stat(filename); erro == nil && fstat.Size() > logfile_maxsize {
						f, err := os.OpenFile(filename, os.O_TRUNC, 0664)
						if err == nil {
							f.Close()
						}
					}
				}
			}
		}
		// Defer next scan for a bit.
		time.Sleep(clearPeriod) // Make this tunable
	}
}
