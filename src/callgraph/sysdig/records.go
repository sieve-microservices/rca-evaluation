package sysdig

import (
    "encoding/csv"
    "fmt"
    "io"
    "log"
//    "net"
//    "strconv"
    "strings"
)

const (
    RX = iota
    TX
)

type Record struct {

    container_name  string
    direction       int
    port_num        string
}

func ParseOutput(result string) ([]Record, error) {
    r := csv.NewReader(strings.NewReader(result))
    // httplog chisel uses spaces as delimiters
    r.Comma = ' '

    var records []Record
    for {
        record, err := r.Read()
        if err == io.EOF {
            break
        }
        if err != nil {
            log.Fatal(err)
        }

        // direction in the httplog chisel is '>" (transmit) '<' (receive)
        var direction int

        if record[3] == "<" {
            direction = RX
        } else {
            direction = TX
        }

        // container name
        container_name := record[2]
        // extract port number from url (well-known port number of openstack 
        // services)
        mangled_url := strings.SplitN(record[5], ":", 2)
        // skip processing when no port can be extracted
        if len(mangled_url) < 2 {
            fmt.Errorf("couldn't extract port from '%s'", record[4])
            continue
        }

        port_num := strings.SplitN(mangled_url[1], "/", 2)[0]

        records = append(records, Record{
            container_name,
            direction,
            port_num,
        })
    }
    return records, nil
}
