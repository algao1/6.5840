# 6.5840 Labs

## Random Thoughts

### Lab 2A: leader election (moderate)

Spent about ~4 hours on the second attempt, but definitely took longer the first time around.
Completed Jan 23 (2024-01-23).

- Lots of small details to watch out for
  - The tester requires
    - The leader send heartbeats <= 10 times a second, so set a reasonable timeout for heartbeats
    - Similarly, need leadership to converge within 5 seconds
  - Don't return immediately in the handlers after args.Term > currentTerm
  - Be careful of where you reset the LastElectionEvent time
    - From the Raft docs it says: "If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate."
    - In particular, only when
      - We get an AppendEntries RPC from the current leader (if outdated args, do not reset)
      - We are starting an election (transitioning to candidate)
      - We are granting a vote to another peer
  - If you are candidate, the election timer dictates when to start another election (start RPCs in separate goroutine, since they might be delayed or dropped)
  - Any replies with term > currentTerm, convert to follower
- I also found it helpful to write it as an actual state machine and use tick functions

### Lab 2B: log replication (hard)

Goal: Implement log replication.

- "If election timeout elapses without **receiving AppendEntries RPC from current leader** or granting vote to candidate: convert to candidate"
- If you're using a channel to send updates on when to commit, be careful about being blocked while inside a critical section. Had some issues because of mutexes
