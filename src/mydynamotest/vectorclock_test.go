package mydynamotest

import (
    "mydynamo"
    "testing"
)

func TestBasicVectorClock(t *testing.T) {
    t.Logf("Starting TestBasicVectorClock")

    //create two vector clocks
    clock1 := mydynamo.NewVectorClock()
    clock2 := mydynamo.NewVectorClock()

    //Test for equality
    if !clock1.Equals(clock2) {
        t.Fail()
        t.Logf("Vector Clocks were not equal")
    }
}

func TestLessthan(t *testing.T) {
    t.Logf("Starting TestBasicVectorClock")

    //create two vector clocks
    clock0 := mydynamo.NewVectorClock()
    clock1 := mydynamo.NewVectorClock()
    clock1.ClockNode["a"] = 1
    clock2 := mydynamo.NewVectorClock()
    clock2.ClockNode["a"] = 2
    clock2.ClockNode["b"] = 1

    //Test for equality
    if !clock0.LessThan(clock1) {
        t.Fail()
        t.Logf("Vector Clocks were not equal")
    }

    if !clock1.LessThan(clock2) {
        t.Fail()
        t.Logf("Vector Clocks were not equal")
    }

    if clock2.LessThan(clock1) {
        t.Fail()
        t.Logf("Vector Clocks were not equal")
    }
}
