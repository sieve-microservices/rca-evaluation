package sysdig

import (
    "fmt"
    "net"
    "strconv"
    "os"
)

type Records []Record

type Service struct {
    Name string
    Ips  []net.IP
}

type Connection struct {
    From string
    To   string
}

func check(e error) {
    if e != nil {
        panic(e)
    }
}

func (records Records) PrintGraph(port_map map[int]string) {

    connections := make(map[string]Connection)

    for _, r := range records {
        // get the container name
        a := r.container_name
        // convert the port number to int
        port_num, err := strconv.Atoi(r.port_num)
        if err != nil {
            fmt.Errorf("failed to convert port number : '%s'")
            continue
        }

        // replace the port number by a service name and write the connection 
        // info
        if b, ok := port_map[port_num]; ok {

            if r.direction == TX {
                connections[a + ":" + b] = Connection{a, b}
            } else {
                connections[b + ":" + a] = Connection{b, a}
            }
        }
    }

    // write a .dot file w/ the callgraph
    callgraph_file, err := os.Create("openstack-callgraph.dot")
    check(err)
    defer callgraph_file.Close()

    fmt.Printf("digraph G {\n")
    _, err = callgraph_file.WriteString("digraph G {\n")
    for _, connection := range connections {
        fmt.Printf("\"%s\" -> \"%s\"\n", connection.From, connection.To)
        _, err = callgraph_file.WriteString(fmt.Sprintf("    \"%s\" -> \"%s\"\n", connection.From, connection.To))
    }
    fmt.Printf("}\n")
    _, err = callgraph_file.WriteString("}\n")
}
