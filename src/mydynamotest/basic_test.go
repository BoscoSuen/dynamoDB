package mydynamotest

import (
    "mydynamo"
    "testing"
    "time"
)

func TestBasicPutAndGet(t *testing.T) {
    t.Logf("Starting basic Put and Get test")

    //Test initialization
    //Note that in the code below, dynamo servers will use the config file located in src/mydynamotest
    cmd := InitDynamoServer("./myconfig.ini")
    ready := make(chan bool)

    //starts the Dynamo nodes, and get ready to kill them when done with the test
    go StartDynamoServer(cmd, ready)
    defer KillDynamoServer(cmd)

    //Wait for the nodes to finish spinning up.
    time.Sleep(3 * time.Second)
    <-ready

    //Create a client that connects to the first server
    //This assumes that the config file specifies 8080 as the starting port
    clientInstance := MakeConnectedClient(8080)

    //Put a value on key "s1"
    clientInstance.Put(PutFreshContext("s1", []byte("abcde")))

    //Get the value back, and check if we successfully retrieved the correct value
    gotValuePtr := clientInstance.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestBasicPutAndGet: Returned nil")
    }
    gotValue := *gotValuePtr
    if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
        t.Fail()
        t.Logf("TestBasicPutAndGet: Failed to get value")
    }
}

// Conflict case in the write-up
func TestBasicConflict(t *testing.T) {
    t.Logf("Starting Conflict test")

    //Test initialization
    //Note that in the code below, dynamo servers will use the config file located in src/mydynamotest
    cmd := InitDynamoServer("./myconfig.ini")
    ready := make(chan bool)

    //starts the Dynamo nodes, and get ready to kill them when done with the test
    go StartDynamoServer(cmd, ready)
    defer KillDynamoServer(cmd)

    //Wait for the nodes to finish spinning up.
    time.Sleep(3 * time.Second)
    <-ready

    // Create client X,Y,Z that connects to the first server
    // This assumes that the config file specifies 8080,8081,8082 as the starting port
    X := MakeConnectedClient(8080)
    Y := MakeConnectedClient(8081)
    Z := MakeConnectedClient(8082)

    //Put a value on key "s1"
    X.Put(PutFreshContext("k1", []byte("value1")))

    //Get the value back, and check if we successfully retrieved the correct value
    gotValuePtr := X.Get("k1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestBasicConflict: Returned nil")
        return
    }
    gotValue := *gotValuePtr
    if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("value1")) {
        t.Fail()
        t.Logf("TestBasicConflict: Failed to get value")
    }

    // Update k1 value to value2 and check
    X.Put(mydynamo.NewPutArgs("k1",gotValue.EntryList[0].Context ,[]byte("value2")))

    gotValuePtr = X.Get("k1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestBasicConflict: Returned nil")
        return
    }
    gotValue = *gotValuePtr
    if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("value2")) {
        t.Fail()
        t.Logf("TestBasicConflict: Failed to get value")
    }

    X.Gossip()

    // Create conflict
    Y.Put(PutFreshContext("s1", []byte("value3")))

    Z.Put(PutFreshContext("s1", []byte("value4")))

    Y.Gossip()
    Z.Gossip()

    // Check k-v pair after gossip
    y := *Y.Get("k1")
    if len(y.EntryList) != 1 || !valuesEqual(y.EntryList[0].Value, []byte("value2")) {
        t.Fail()
        t.Logf("TestBasicConflict: gossip fail")
    }

    y = *Y.Get("s1")
    if len(y.EntryList) != 2 {
        t.Fail()
        t.Logf("TestBasicConflict: gossip fail")
    }

    z := *Z.Get("k1")
    if len(z.EntryList) != 1 || !valuesEqual(z.EntryList[0].Value, []byte("value2")) {
        t.Fail()
        t.Logf("TestBasicConflict: gossip fail")
    }

    z = *Z.Get("s1")
    if len(z.EntryList) != 2 {
        t.Fail()
        t.Logf("TestBasicConflict: gossip fail")
    }


    // Check conflict
    gotValuePtr = X.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestBasicConflict: Returned nil")
        return
    }
    gotValue = *gotValuePtr
    if len(gotValue.EntryList) != 2 {
        t.Fail()
        t.Logf("TestBasicConflict: Two conflict values have not been kept")
    }

    // Fix conflit
    X.Put(mydynamo.NewPutArgs("s1",gotValue.EntryList[0].Context ,[]byte("value5")))
    gotValuePtr = X.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestBasicConflict: Returned nil")
        return
    }
    gotValue = *gotValuePtr
    if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("value5")) {
        t.Fail()
        t.Logf("TestBasicConflict: Conflict did not solved")
    }

    // Check other server after X put value5
    X.Gossip()
    gotValuePtr = Y.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestBasicConflict: Returned nil")
        return
    }
    gotValue = *gotValuePtr
    if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("value5")) {
        t.Fail()
        t.Logf("TestBasicConflict: Conflict did not solved")
    }

    gotValuePtr = Z.Get("s1")
    if gotValuePtr == nil {
        t.Fail()
        t.Logf("TestBasicConflict: Returned nil")
        return
    }
    gotValue = *gotValuePtr
    if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("value5")) {
        t.Fail()
        t.Logf("TestBasicConflict: Conflict did not solved")
    }
}