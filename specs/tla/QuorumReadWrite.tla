---- MODULE QuorumReadWrite ----
EXTENDS Naturals, FiniteSets

\* A compact model of Dynamo-style quorum reads and writes for one key.
\*
\* The model abstracts away transport, storage engine details, coordinator
\* selection, and anti-entropy.  It keeps only the state needed to verify the
\* quorum-intersection argument used by Toy Dynamo when R + W > N.

CONSTANTS Nodes, Values, R, W, MaxClock

VARIABLES Store, Acked, Clock, LastRead

Vars == <<Store, Acked, Clock, LastRead>>

NodeCount == Cardinality(Nodes)

QuorumSets(q) == {s \in SUBSET Nodes : Cardinality(s) >= q}
ReadQuorums == QuorumSets(R)
WriteQuorums == QuorumSets(W)

ZeroClock == [n \in Nodes |-> 0]

Increment(vc, n) ==
  [m \in Nodes |-> IF m = n THEN vc[m] + 1 ELSE vc[m]]

ClockLeq(a, b) ==
  \A n \in Nodes : a[n] <= b[n]

ClockLt(a, b) ==
  /\ ClockLeq(a, b)
  /\ \E n \in Nodes : a[n] < b[n]

Version ==
  [value : Values, clock : [Nodes -> 0..MaxClock]]

DominatesOrEqual(v1, v2) ==
  ClockLeq(v2.clock, v1.clock)

StrictlyDominates(v1, v2) ==
  ClockLt(v2.clock, v1.clock)

NodeVersions(rs) ==
  UNION {Store[n] : n \in rs}

VisibleVersions(versions) ==
  {v \in versions : ~(\E other \in versions : StrictlyDominates(other, v))}

HoldsOrDominates(n, v) ==
  \E local \in Store[n] : DominatesOrEqual(local, v)

ReadResult(rs, result) ==
  result = VisibleVersions(NodeVersions(rs))

Init ==
  /\ Store = [n \in Nodes |-> {}]
  /\ Acked = {}
  /\ Clock = ZeroClock
  /\ LastRead = {}

Write ==
  \E writer \in Nodes :
  \E value \in Values :
  \E ws \in WriteQuorums :
    /\ Clock[writer] < MaxClock
    /\ LET newClock == Increment(Clock, writer) IN
       LET version == [value |-> value, clock |-> newClock] IN
       /\ Store' = [n \in Nodes |->
             IF n \in ws THEN Store[n] \cup {version} ELSE Store[n]]
       /\ Acked' = Acked \cup {version}
       /\ Clock' = newClock
       /\ UNCHANGED LastRead

Read ==
  \E rs \in ReadQuorums :
    \E result \in SUBSET Version :
      /\ ReadResult(rs, result)
      /\ LastRead' = result
    /\ UNCHANGED <<Store, Acked, Clock>>

Next == Write \/ Read

Spec == Init /\ [][Next]_Vars

TypeOK ==
  /\ Store \in [Nodes -> SUBSET Version]
  /\ Acked \in SUBSET Version
  /\ Clock \in [Nodes -> 0..MaxClock]
  /\ LastRead \in SUBSET Version
  /\ R \in 1..NodeCount
  /\ W \in 1..NodeCount

\* If R + W > N, every acknowledged write is present in, or causally
\* dominated by a version present in, every possible read quorum.
NoLostAcknowledgedWrite ==
  (R + W > NodeCount) =>
    \A v \in Acked :
      \A rs \in ReadQuorums :
        \E n \in rs : HoldsOrDominates(n, v)

=============================================================================
