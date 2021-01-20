package mydynamo

type VectorClock struct {
    ClockNode   map[string]int
    // mutex       *sync.RWMutex
}

// Creates a new VectorClock
func NewVectorClock() VectorClock {
    return VectorClock { ClockNode: make(map[string]int)}
}

// Returns true if the other VectorClock is causally descended from this one
func (s VectorClock) LessThan(otherClock VectorClock) bool {
    if s.Equals(otherClock) {
        return false
    }

    for nodeId, version := range s.ClockNode {
        if _, ok := otherClock.ClockNode[nodeId]; ok {
            if version > otherClock.ClockNode[nodeId]{
                return false
            }
        } else {
            if version > 0 {
                return false
            }
        }
    }

    return true
}

// Returns true if neither VectorClock is concurrent with the other
func (s VectorClock) Concurrent(otherClock VectorClock) bool {
    return !s.LessThan(otherClock) && !otherClock.LessThan(s)
}

// Increments this VectorClock at the element associated with nodeId
func (s *VectorClock) Increment(nodeId string) {
    s.ClockNode[nodeId]++
}

// Changes this VectorClock to be causally descended from all VectorClocks in clocks
// Combine should return a vector clock >= to all clocks, including s.
func (s *VectorClock) Combine(clocks []VectorClock) {
    for _, clock := range clocks {
        for nodeId, version := range clock.ClockNode {
            if _, ok := s.ClockNode[nodeId]; ok {
                s.ClockNode[nodeId] = Max(s.ClockNode[nodeId], version)
            } else {
                s.ClockNode[nodeId] = version
            }
        }
    }
}

// Tests if two VectorClocks are equal
func (s *VectorClock) Equals(otherClock VectorClock) bool {
    for nodeId, version := range s.ClockNode {
        if _, ok := otherClock.ClockNode[nodeId]; ok {
            if version != otherClock.ClockNode[nodeId]{
                return false
            }
        } else {
            if version > 0 {
                return false
            }
        }
    }
    for nodeId, version := range otherClock.ClockNode {
        if _, ok := s.ClockNode[nodeId]; ok {
            if version != s.ClockNode[nodeId]{
                return false
            }
        } else {
            if version > 0 {
                return false
            }
        }
    }
    return true
}
