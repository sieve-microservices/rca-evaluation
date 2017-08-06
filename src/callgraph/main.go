package main

import (
    "flag"
    "fmt"
    "os"
    "os/exec"
    "syscall"
    "path/filepath"
    "strings"
    "bufio"
    "strconv"

    "./sysdig"
)

var (
    rally_bin               = flag.String("rally-bin", "/home/antonio/rally/bin/rally", "rally executable.")
    rally_dir               = flag.String("rally-dir", "/home/antonio/rally/samples/tasks/scenarios", "path to rally scenarios dir, were different .json rally scenarios should be kept.")
    rally_task_path         = flag.String("rally-task", "nova/boot-and-delete.json", "path to rally task .json file, starting from RALLY_SCENARIOS_DIR. e.g. '--task-path \"nova/boot-and-delete.json\"'")
    port_file_path          = flag.String("openstack-ports", "openstack.ports", "file with mappings between ports and openstack service names")

    rancherHost      = flag.String("rancher-host", "slfy80.local:8080", "host of rancher server")
    rancherAccessKey = flag.String("rancher-access-key", os.Getenv("RANCHER_ACCESS_KEY"), "api access key")
    rancherSecretKey = flag.String("rancher-secret-key", os.Getenv("RANCHER_SECRET_KEY"), "api secret key")
)

func printCommand(cmd *exec.Cmd) {
    fmt.Printf("==> Executing: %s\n", strings.Join(cmd.Args, " "))
}

func printError(err error) {
    if err != nil {
        os.Stderr.WriteString(fmt.Sprintf("==> Error: %s\n", err.Error()))
    }
}

func printOutput(outs []byte) {
    if len(outs) > 0 {
        fmt.Printf("==> Output: %s\n", string(outs))
    }
}

func parseArgs() (hosts []string) {

    flag.Parse()
    args := flag.Args()

    if len(args) < 1 {
        fmt.Fprintf(os.Stderr, "%s : [ERROR] usage : %s <host 1> ... <host n>\n", os.Args[0], os.Args[0])
        os.Exit(1)
    }

    if *rancherAccessKey == "" {
        fmt.Fprintf(os.Stderr, "%s : [ERROR] set RANCHER_ACCESS_KEY environment variable", os.Args[0])
        os.Exit(1)
    }

    if *rancherSecretKey == "" {
        fmt.Fprintf(os.Stderr, "%s : [ERROR] set RANCHER_SECRET_KEY environment variable", os.Args[0])
        os.Exit(1)
    }

    // return the list of hosts where openstack is running
    return args[0:]
}

func extract_openstack_ports() (map[int]string, error) {
    // initialize a map of port numbers into openstack services
    port_map := make(map[int]string)

    // open the .ports file to extract the mappings for this kolla installation
    port_file, err := os.Open(*port_file_path)
    if err != nil {
        return nil, fmt.Errorf("failed to open openstack ports file")
    }
    // not sure what this does...
    defer port_file.Close()

    // extract mappings and fill port_map
    port_file_scanner := bufio.NewScanner(port_file)
    for port_file_scanner.Scan() {

        line := strings.Split(port_file_scanner.Text(), ":")

        // service string first
        service := line[0]
        // 1 or more port numbers second
        port_nums := strings.Split(line[1], ",")
        for _, port_num := range port_nums {

            _port_num, err := strconv.Atoi(port_num)
            if err != nil {
                continue
            }

            port_map[_port_num] = service
        }
    }

    return port_map, nil
}

func die(message string, args ...interface{}) {
    fmt.Fprintf(os.Stderr, message, args...)
    fmt.Fprint(os.Stderr, "\n")
    os.Exit(1)
}

func main() {

    hosts := parseArgs()

    // starts sysdig on all hosts (sysdig is basically strace + tcpdump 
    // + htop + iftop + lsof + transaction tracing, everything you need for 
    // callgraph generation)
    cmd, err := sysdig.Start(hosts)
    if err != nil {
        die("failed to start sysdig: %v", err)
    }

    // build the rally command
    rally_cmd := exec.Command(*rally_bin, "task", "start", filepath.Join(*rally_dir, *rally_task_path))
    // print it to make sure it's built ok...
    printCommand(rally_cmd)
    // execute the command, waiting for it to finish
    //err = rally_cmd.Run()
    output, rally_err := rally_cmd.CombinedOutput()
    // print output and errors (if any)
    printOutput(output)
    if rally_err != nil {
        die("rally is out of gas? : %v", rally_err)
    }

    // stop sysdig in hosts
    cmd.SendStopSignal()

    // collect the measurements from the hosts
    var records []sysdig.Record
    for range hosts {
        select {
        case result := <-cmd.Results:
            if exitError, ok := result.Error.(*exec.ExitError); ok {
                waitStatus := exitError.Sys().(syscall.WaitStatus)
                if waitStatus != 130 { // Interrupt
                    fmt.Printf("[%s] sysdig failed with %v\n", result.Host.Name, result.Error, result.Output)
                }
            }

            // records should be transformed into the format
            // [src_container_name] > [dst_port_name]
            // [dst_container_name] < [src_port_name]
            r, err := sysdig.ParseOutput(result.Output)
            if err != nil {
                die("error parsing results from %s", result.Host.Name)
            }
            records = append(records, r...)
        }
    }
    // translate port numbers into openstack services
    openstack_ports, err := extract_openstack_ports()
    if err != nil {
        die("error extracting openstack ports: %s", err)
    }
    (sysdig.Records(records)).PrintGraph(openstack_ports)
}
