package mydynamo

// Removes an element at the specified index from a list of ObjectEntry structs
func remove(list []ObjectEntry, index int) []ObjectEntry {
    return append(list[:index], list[index+1:]...)
}

// Returns true if the specified list of ints contains the specified item
func contains(list []int, item int) bool {
    for _, v := range list {
        if v == item {
            return true
        }
    }
    return false
}

// Rotates a preference list by one, so that we can give each node a unique preference list
func RotateServerList(list []DynamoNode) []DynamoNode {
    return append(list[1:], list[0])
}

// Creates a new Context with the specified Vector Clock
func NewContext(vClock VectorClock) Context {
    return Context{
        Clock: vClock,
    }
}

// Creates a new PutArgs struct with the specified members.
func NewPutArgs(key string, context Context, value []byte) PutArgs {
    return PutArgs{
        Key:     key,
        Context: context,
        Value:   value,
    }
}

// Creates a new DynamoNode struct with the specified members
func NewDynamoNode(addr string, port string) DynamoNode {
    return DynamoNode{
        Address: addr,
        Port:    port,
    }
}

// Return the max value of two values
func Max(x, y int) int {
    if x >= y {
        return x
    } else {
        return y
    }
}

// Compare two []byte values
func Equals(x, y []byte) bool {
    if len(x) != len(y) {
        return false
    }
    for i := 0; i < len(x); i ++ {
        if x[i] != y[i] {
            return false
        }
    }
    return true
}

// Find if element in the entry list
func Contains(entryList []ObjectEntry, value []byte) bool {
    for _, entry := range entryList {
        if Equals(entry.Value, value) {
            return true
        }
    }
    return false
}