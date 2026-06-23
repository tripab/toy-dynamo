---- MODULE SloppyHandoff ----
EXTENDS Naturals, FiniteSets

\* Sloppy quorum and hinted handoff model for one key.
\*
\* Primaries are the normal preference-list replicas.  When too few primaries
\* are available, a write may include live non-primary substitute nodes.  Each
\* unavailable primary gets a hint stored on one of the write recipients; when
\* that target recovers, the hint is delivered exactly to that target.

CONSTANTS Nodes, Primaries, Values, W, MaxClock

VARIABLES Store, Hints, Delivered, Available, Acked, Clock

Vars == <<Store, Hints, Delivered, Available, Acked, Clock>>

NodeCount == Cardinality(Nodes)
LivePrimaries == Primaries \cap Available
LiveSubstitutes == Available \ Primaries

ZeroClock == [n \in Nodes |-> 0]

Increment(vc, n) ==
  [m \in Nodes |-> IF m = n THEN vc[m] + 1 ELSE vc[m]]

Version ==
  [value : Values, clock : [Nodes -> 0..MaxClock]]

Hint ==
  [target : Primaries, holder : Nodes, version : Version]

Delivery ==
  [hint : Hint, deliveredTo : Nodes]

SubstituteSets ==
  {s \in SUBSET LiveSubstitutes : Cardinality(LivePrimaries \cup s) >= W}

Init ==
  /\ Store = [n \in Nodes |-> {}]
  /\ Hints = {}
  /\ Delivered = {}
  /\ Available = Nodes
  /\ Acked = {}
  /\ Clock = ZeroClock

FailNode ==
  \E n \in Available :
    /\ Available' = Available \ {n}
    /\ UNCHANGED <<Store, Hints, Delivered, Acked, Clock>>

RecoverNode ==
  \E n \in Nodes \ Available :
    /\ Available' = Available \cup {n}
    /\ UNCHANGED <<Store, Hints, Delivered, Acked, Clock>>

SloppyWrite ==
  \E writer \in Available :
  \E value \in Values :
  \E substitutes \in SubstituteSets :
    LET writeSet == LivePrimaries \cup substitutes IN
    LET failedPrimaries == Primaries \ Available IN
      /\ Clock[writer] < MaxClock
      /\ Cardinality(writeSet) >= W
      /\ \E holderFor \in [failedPrimaries -> writeSet] :
          LET newClock == Increment(Clock, writer) IN
          LET version == [value |-> value, clock |-> newClock] IN
          /\ Store' = [n \in Nodes |->
                IF n \in writeSet THEN Store[n] \cup {version} ELSE Store[n]]
          /\ Hints' = Hints \cup
                {[target |-> p, holder |-> holderFor[p], version |-> version] :
                    p \in failedPrimaries}
          /\ Acked' = Acked \cup {version}
          /\ Clock' = newClock
          /\ UNCHANGED <<Delivered, Available>>

DeliverHint ==
  \E h \in Hints :
    /\ h.target \in Available
    /\ Store' = [n \in Nodes |->
          IF n = h.target THEN Store[n] \cup {h.version} ELSE Store[n]]
    /\ Hints' = Hints \ {h}
    /\ Delivered' = Delivered \cup {[hint |-> h, deliveredTo |-> h.target]}
    /\ UNCHANGED <<Available, Acked, Clock>>

Next == FailNode \/ RecoverNode \/ SloppyWrite \/ DeliverHint

Spec == Init /\ [][Next]_Vars

TypeOK ==
  /\ Primaries \subseteq Nodes
  /\ W \in 1..NodeCount
  /\ Store \in [Nodes -> SUBSET Version]
  /\ Hints \in SUBSET Hint
  /\ Delivered \in SUBSET Delivery
  /\ Available \in SUBSET Nodes
  /\ Acked \in SUBSET Version
  /\ Clock \in [Nodes -> 0..MaxClock]

HintsStayWithWriteRecipient ==
  \A h \in Hints :
    /\ h.holder \in Nodes
    /\ h.version \in Store[h.holder]

HintsTargetPrimaries ==
  \A h \in Hints \cup {d.hint : d \in Delivered} :
    h.target \in Primaries

HintsDeliveredToCorrectTarget ==
  \A d \in Delivered :
    /\ d.deliveredTo = d.hint.target
    /\ d.hint.version \in Store[d.hint.target]

HintsDeliveredAtMostOnce ==
  \A h \in Hint :
    Cardinality({d \in Delivered : d.hint = h}) <= 1

=============================================================================
