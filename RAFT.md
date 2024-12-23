# DisSys-A2 `Raft`

> ***Raft*** is a protocol for implementing distributed consensus.

`node states`

- `Follower`
- `Candidate`
- `Leader`

All nodes start in the follower state

if followers ***don't hear from*** a leader, then they can become a `candidate`

the candidate then ***requests*** votes from other nodes

nodes will ***reply*** with their vote

the candidate becomes the leader if it gets votes from a ***majority*** of nodes

the process above is called ***`Leader Election`***

***all changes*** to the system now go through the ***leader***

each change is added as an entry in the node's ***log***

the log entry is currently ***uncommitted*** so it won't update the node's value

to commit the entry the node first ***replicates*** it to the ***follower*** nodes...

then the leader waits until a ***majority*** of nodes have ***written*** the entry

the entry is now ***committed*** on the ***leader*** node and the leader node value is updated to the changed value

the leader then notified the ***followers*** that the entry is ***committed***, all follower nodes value are updated to the changed value

the cluster has now come to ***consensus*** about the system state

the process above is called ***`Log Replication`***

## Leader Election

In raft there are ***two timeout*** settings which control elections

- ***election timeout***  
the election timeout is the amount of time a follower *waits until becoming a candidate*  
the election timeout is randomized to be between 150ms adn 300ms  
*after* the election timeout the follower **becomes a candidate** and starts a new election term...  
...votes for itself...  
... and sends out **Request Vote** messages to other nodes  
if the receiving node **hasn't voted** yet **in this term** then it votes for the candidate...  
and the node **resets** its election timeout  
once a candidate has a **majority** of votes it becomes leader  
the leader begins sending out ***Append Entries*** messages to its followers  

`note` :  
requiring a **majority** of votes guarantees that ***only one*** leader can be elected per term

- ***heartbeat timeout***  
these aforementioned messages are sent ***in intervals*** specified by the ***heartbeat timeout***  
followers then respond to each ***Append Entries*** message and **reset** the heartbeat timeout  
this election term will continue until a follower ***stops receving heartbeats*** and becomes a **candidate**  

## Log Replication

once we have a leader elected we need to replicate all changes to our system to **all nodes**

this is done by using the same **Append Entries** message that was used for **heartbeats**

first a client sends a change to the **leader**

the change is *appened* to the **leader's log**

then the change is sent to the followers on the ***next heartbeat***

an entry is **committed** once a **majority** of followers acknowledge it

then a response is sent to the client

`note` :  
Raft can even stay consistent in the face of network partitions

## 注释

在 ***log replication*** 中，当网络故障将原来的集群划分为两个分组，在网络恢复后，有较老的leader的集群会将所有 `uncommitted` 的 `logEntry` **roll back**。然后按照新leader的 `logEntries` 进行修改。

## 一些规则

- the voter denies its vote if its own log is more **up-to-date** than that of the candidate. Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in the logs. If the logs have last entries with different terms, then the log with the later term is more up-to-date. If the logs bend with the same term, then whichever log is longer is more up-to-date.
- Raft never commits log entries from previous terms by counting replicas. Only log entries from the leader’s **current** term are committed by counting replicas.
- log entries **retain** their **original term numbers** when a leader replicates entries from previous terms.
- leaders **never** remove entries, and followers **only** remove entries if they conflict with the leader.
