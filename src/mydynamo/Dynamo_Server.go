package mydynamo

import (
    "errors"
    "log"
    "net"
    "net/http"
    "net/rpc"
    "strconv"
    "sync"
    "time"
)

type DynamoServer struct {
    /*------------Dynamo-specific-------------*/
    wValue         int          // Number of nodes to write to on each Put
    rValue         int          // Number of nodes to read from on each Get
    preferenceList []DynamoNode // Ordered list of other Dynamo nodes to perform operations o
    selfNode       DynamoNode   // This node's address and port info
    nodeID         string       // ID of this node
    clock          VectorClock  // Vector clock element that is associated with this node
    dynamoMap      map[string]*DynamoResult     // Internal map to store keys and values, along with associated vector clocks.
    crashed        bool
    mutex          sync.Mutex
}

func (s *DynamoServer) SendPreferenceList(incomingList []DynamoNode, _ *Empty) error {
    s.preferenceList = incomingList
    return nil
}

// Forces server to gossip
// As this method takes no arguments, we must use the Empty placeholder
func (s *DynamoServer) Gossip(_ Empty, _ *Empty) error {
    // If server crashed, return an error
    if s.crashed {
        return errors.New("server crashed")
    }
    var err error

    // Get all other servers from preferenceList
    for i, node := range s.preferenceList {
        if s.preferenceList[i] == s.selfNode {
            continue
        }
    NodeLoop:
        for key, result := range s.dynamoMap {
            for _, entry := range result.EntryList {
                var value PutArgs
                value.Key = key
                value.Context = entry.Context
                value.Value = entry.Value

                conn, err := rpc.DialHTTP("tcp", node.Address + ":" + node.Port)
                if err != nil {
                    log.Println("RPC conn failed: ", err)
                    break NodeLoop
                }

                var result bool
                log.Println("current server: " + s.nodeID + "put value: " + string(value.Value) + "to port: " + node.Port)
                err = conn.Call("MyDynamo.PutLocal", value, &result)
                if err != nil {
                    log.Println("Gossip failed: ", err)
                } else {
                    log.Println("Gossip succeed")
                }
            }
        }
    }
    return err
}

// Makes server unavailable for some seconds
func (s *DynamoServer) Crash(seconds int, success *bool) error {
    s.crashed = true
    time.Sleep(time.Second * time.Duration(seconds))
    s.crashed = false
    *success = true
    return nil
}

// Put a file to this local server
func (s *DynamoServer) PutLocal(value PutArgs, result *bool) error {
    // If server crashed, return an error
    if s.crashed {
        return errors.New("server crashed")
    }

    log.Println("put local start")

    var err error
    if _, ok := s.dynamoMap[value.Key]; ok {
        // The Node has already stored associated with the specified key.
        log.Println("already has the key")
        for _, entry := range s.dynamoMap[value.Key].EntryList {
            // Check causality.
            log.Println("entry clock: ", entry.Context.Clock.ClockNode)
            log.Println("value clock: ", value.Context.Clock.ClockNode)
            if value.Context.Clock.LessThan(entry.Context.Clock) {
                return errors.New("put failed, context not causally descended")
            }
        }

        for _, entry := range s.dynamoMap[value.Key].EntryList {
            // Only update the older one
            if entry.Context.Clock.LessThan(value.Context.Clock) {
                newEntryList := make([]ObjectEntry, 0)
                var newEntry ObjectEntry
                newEntry.Context = value.Context
                newEntry.Value = value.Value
                newEntryList = append(newEntryList, newEntry)

                s.dynamoMap[value.Key].EntryList = newEntryList       // Update local entryList

                s.clock.Increment(s.nodeID)

                log.Println("put local end")
                log.Println("-------------------")
                return err
            }
        }
        log.Println("Have conflict")
        //for _, entry := range s.dynamoMap[value.Key].EntryList {
        //    log.Println("conflict, entry: ", string(entry.Value))
        //    log.Println("conflict, value: ", string(value.Value))
        //    if !Equals(entry.Value, value.Value) {
        //        var newEntry ObjectEntry
        //        newEntry.Context = value.Context
        //        newEntry.Value = value.Value
        //        s.dynamoMap[value.Key].EntryList = append(s.dynamoMap[value.Key].EntryList, newEntry)
        //    }
        //}
        if !Contains(s.dynamoMap[value.Key].EntryList, value.Value) {
            var newEntry ObjectEntry
            newEntry.Context = value.Context
            newEntry.Value = value.Value
            s.dynamoMap[value.Key].EntryList = append(s.dynamoMap[value.Key].EntryList, newEntry)
        }

    } else {
        // Put new value
        log.Println("put new value")
        newEntryList := make([]ObjectEntry, 0)
        var newEntry ObjectEntry
        newEntry.Context = value.Context
        newEntry.Value = value.Value
        newEntryList = append(newEntryList, newEntry)
        var dynamoResult DynamoResult
        dynamoResult.EntryList = newEntryList

        s.dynamoMap[value.Key] = &dynamoResult

        s.clock.Increment(s.nodeID)
    }

    log.Println("put local end")
    log.Println("-------------------")

    return err
}

// Put a file to this server and W other servers
func (s *DynamoServer) Put(value PutArgs, result *bool) error {
    // If server crashed, return an error
    if s.crashed {
        return errors.New("server crashed")
    }

    log.Println("Put Start")

    s.mutex.Lock()
    defer func() {
        s.mutex.Unlock()
    }()
    var err error
    if _, ok := s.dynamoMap[value.Key]; ok {
        // The Node has already stored associated with the specified key.
        //log.Println("Server already has the key")
        for _, entry := range s.dynamoMap[value.Key].EntryList {
            // Check causality.
            if value.Context.Clock.LessThan(entry.Context.Clock) || value.Context.Clock.Equals(entry.Context.Clock) {
                return errors.New("put failed, context not causally descended")
            }
        }
        for _, entry := range s.dynamoMap[value.Key].EntryList {
            // Only update the older one.
            node := entry.Context.Clock.ClockNode
            for k,v := range node {
                log.Println("K: " + k + "v:" + strconv.Itoa(v))
            }
            //log.Println("entry clock: ", entry.Context.Clock.ClockNode)
            //log.Println("value clock: ", value.Context.Clock.ClockNode)
            if entry.Context.Clock.LessThan(value.Context.Clock) {
                // Put into local k-v store.
                log.Println("Current entry less than the value")
                newEntryList := make([]ObjectEntry, 0)
                var newEntry ObjectEntry
                newEntry.Context = value.Context
                newEntry.Value = value.Value
                newEntryList = append(newEntryList, newEntry)

                s.dynamoMap[value.Key].EntryList = newEntryList       // Update local entryList

                // Replicate value to W-1 other nodes via RPC call.
                err = s.putReplication(value)

                s.clock.Increment(s.nodeID)

                log.Println("put end")
                log.Println("---------------------")
                return err
            }
        }
        log.Println("Have conflict")
        //for _, entry := range s.dynamoMap[value.Key].EntryList {
        //    log.Println("conflict, entry: ", string(entry.Value))
        //    log.Println("conflict, value: ", string(value.Value))
        //    if !Equals(entry.Value, value.Value) {
        //        var newEntry ObjectEntry
        //        newEntry.Context = value.Context
        //        newEntry.Value = value.Value
        //        s.dynamoMap[value.Key].EntryList = append(s.dynamoMap[value.Key].EntryList, newEntry)
        //    }
        //}
        if !Contains(s.dynamoMap[value.Key].EntryList, value.Value) {
           var newEntry ObjectEntry
           newEntry.Context = value.Context
           newEntry.Value = value.Value
           s.dynamoMap[value.Key].EntryList = append(s.dynamoMap[value.Key].EntryList, newEntry)
        }
    } else {
        // Put new value
        //log.Println("Put new value")
        newEntryList := make([]ObjectEntry, 0)
        var newEntry ObjectEntry
        newEntry.Context = value.Context
        //log.Println("new Context: ", PrintClockNode(newEntry.Context.Clock.ClockNode))
        newEntry.Value = value.Value
        newEntryList = append(newEntryList, newEntry)
        var dynamoResult DynamoResult
        dynamoResult.EntryList = newEntryList

        s.dynamoMap[value.Key] = &dynamoResult

        // Replicate value to W-1 other nodes via RPC call.
        err = s.putReplication(value)

        s.clock.Increment(s.nodeID)
    }

    log.Println("put end")
    log.Println("---------------------")

    return err
}

func (s *DynamoServer) GetLocal(key string, result *DynamoResult) error {
    if s.crashed {
        return errors.New("server crashed")
    }

    if _, ok := s.dynamoMap[key]; !ok {
        return errors.New("Cannot find the key")
    }

    var err error
    for _, entry := range s.dynamoMap[key].EntryList {
        entry.Context = NewContext(s.clock)
        result.EntryList = append(result.EntryList, entry)
    }

    return err
}

//Get a file from this server, matched with R other servers
func (s *DynamoServer) Get(key string, result *DynamoResult) error {
    // If server crashed, return an error
    if s.crashed {
        return errors.New("server crashed")
    }

    log.Println("get start")
    if _, ok := s.dynamoMap[key]; !ok {
        return errors.New("cannot find the key")
    }

    var err error
    for _, entry := range s.dynamoMap[key].EntryList {
        //log.Println("entry clock", entry.Context.Clock.ClockNode)
        entry.Context = NewContext(s.clock)
        result.EntryList = append(result.EntryList, entry)
    }

    // Get pairs from top R-1 other nodes in the preference list
    rCount := 0
    otherClocks := make([]VectorClock, 0)
    var otherResults DynamoResult
    for i := 0; i < s.rValue; i++ {
        if i == len(s.preferenceList) {
            break
        }

        if s.preferenceList[i] == s.selfNode {
            continue
        }

        addr := s.preferenceList[i].Address + ":" + s.preferenceList[i].Port
        conn, err := rpc.DialHTTP("tcp", addr)
        if err != nil {
            log.Println("RPC conn failed: ", err)
            continue
        }

        var dynamoResult DynamoResult
        err = conn.Call("MyDynamo.GetLocal", key, &result)
        if err != nil {
            log.Println("Get failed: ", err)
        } else {
            // Get all clocks to be combined
            for _, entry := range dynamoResult.EntryList {
                otherClocks = append(otherClocks, entry.Context.Clock)
                otherResults.EntryList = append(otherResults.EntryList, entry)
            }
        }
        rCount++
        if rCount == s.rValue - 1 {
            break
        }
    }

    // Check causality
    s.clock.Combine(otherClocks)
    for _, entry := range otherResults.EntryList {
        // leave the concurrent result
        if s.clock.Concurrent(entry.Context.Clock) {
            result.EntryList = append(result.EntryList, entry)
        }
    }

    log.Println("get end")
    log.Println("---------------------")

    return err
}

/* Belows are functions that implement server boot up and initialization */
func NewDynamoServer(w int, r int, hostAddr string, hostPort string, id string) DynamoServer {
    preferenceList := make([]DynamoNode, 0)
    selfNodeInfo := DynamoNode{
        Address: hostAddr,
        Port:    hostPort,
    }
    return DynamoServer{
        wValue:         w,
        rValue:         r,
        preferenceList: preferenceList,
        selfNode:       selfNodeInfo,
        nodeID:         id,
        dynamoMap:      make(map[string]*DynamoResult),
        clock:          NewVectorClock(),
        crashed:        false,
    }
}

func ServeDynamoServer(dynamoServer DynamoServer) error {
    rpcServer := rpc.NewServer()
    e := rpcServer.RegisterName("MyDynamo", &dynamoServer)
    if e != nil {
        log.Println(DYNAMO_SERVER, "Server Can't start During Name Registration")
        return e
    }

    log.Println(DYNAMO_SERVER, "Successfully Registered the RPC Interfaces")

    l, e := net.Listen("tcp", dynamoServer.selfNode.Address+":"+dynamoServer.selfNode.Port)
    if e != nil {
        log.Println(DYNAMO_SERVER, "Server Can't start During Port Listening")
        return e
    }

    log.Println(DYNAMO_SERVER, "Successfully Listening to Target Port ", dynamoServer.selfNode.Address+":"+dynamoServer.selfNode.Port)
    log.Println(DYNAMO_SERVER, "Serving Server Now")

    return http.Serve(l, rpcServer)
}

func (s *DynamoServer) putReplication(value PutArgs) error {
    wCount := 0
    var err error
    for i := 0; i < s.wValue; i++ {
        if i == len(s.preferenceList) || wCount == s.wValue - 1 {
            break
        }

        if s.preferenceList[i] == s.selfNode {
            continue
        }

        addr := s.preferenceList[i].Address + ":" + s.preferenceList[i].Port
        log.Println("Put replica addr: ", addr)
        conn, err := rpc.DialHTTP("tcp", addr)
        if err != nil {
            log.Println("RPC conn failed: ", err)
            continue
        }

        var result bool
        err = conn.Call("MyDynamo.PutLocal", value, &result)
        if err != nil {
            log.Println("Put failed: ", err)
        } else {
            wCount++
        }
    }
    return err
}
