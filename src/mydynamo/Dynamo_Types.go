package mydynamo

//Placeholder type for RPC functions that don't need an argument list or a return value
type Empty struct{}

//Context associated with some value
type Context struct {
    Clock VectorClock
}

//Information needed to connect to a DynamoNOde
type DynamoNode struct {
    Address string
    Port    string
}

//A single value, as well as the Context associated with it
type ObjectEntry struct {
    Context Context
    Value   []byte
}

//Result of a Get operation, a list of ObjectEntry structs
type DynamoResult struct {
    EntryList []ObjectEntry
}

//Arguments required for a Put operation: the key, the context, and the value
type PutArgs struct {
    Key     string
    Context Context
    Value   []byte
}
