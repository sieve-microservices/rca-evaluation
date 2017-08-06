package sysdig

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/mic92/clusterssh"
)

func Start(hostnames []string) (*clusterssh.Command, error) {
	hosts := make([]clusterssh.Host, len(hostnames))
	for i, arg := range hostnames {
		host, err := clusterssh.ParseHost(arg)
		if err != nil {
			return nil, fmt.Errorf("invalid host '%s': %v", arg, err)
		}
		hosts[i] = *host
	}
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	f1, err := os.Open(filepath.Join(dir, "capture-connections.lua"))

	if err != nil {
		return nil, fmt.Errorf("failed to open sysdig plugin")
	}
	sysdigPlugin, err := ioutil.ReadAll(f1)
	if err != nil {
		return nil, fmt.Errorf("failed to read sysdig plugin: %v", err)
	}
	f2, err := os.Open(filepath.Join(dir, "sysdig.sh"))
	if err != nil {
		return nil, fmt.Errorf("failed to open sysdig plugin: %v", err)
	}
	sysdigCommand, err := ioutil.ReadAll(f2)
	if err != nil {
		return nil, fmt.Errorf("failed to read sysdig command: %v", err)
	}
	cluster := clusterssh.Cluster{hosts}
	cmd := cluster.Run(string(sysdigCommand), sysdigPlugin)
	return &cmd, nil
}
