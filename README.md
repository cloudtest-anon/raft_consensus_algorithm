# Raft Consensus Algorithm

Raft is a consensus algorithm for managing a replicated log. Consensus algorithms allow a collection of machines to work as a coherent group that can survive the failures of some of its members.

- Raft implements consensus by first electing a distinguished leader, then giving the leader complete responsibility for managing the replicated log. 
- The leader accepts log entries from clients, replicates them on other servers, and tells servers when it is safe to apply log entries to their state machines.
- A leader can fail or become disconnected from the other servers, in which case a new leader is elected.


Raft implementation can be found under raft/raft.go <br>
This implementation is then used as to build a fault tolerant key-value storage system. Client and server is implemented under kvraft/client.go anf kvraft/server.go
