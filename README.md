The key-value store includes three kinds of operations: Put, Get, and Append.
Append performs the same as Put when the key is not in the store.
Otherwise, it appends new value to the existing value. For example,
1. Put('k', 'a')
2. Append('k', 'bc')
3. Get(k) -> 'abc'

Clients send Put(), Append(), and Get() RPCs to kvpaxos servers. A client can
send an RPC to any of the kvpaxos servers, and should retry by sending to a
different server if there's a failure. Each kvpaxos server contains a replica of
the key/value database; handlers for client Get() and Put()/Append() RPCs; and a
Paxos peer. Paxos takes the form of a library that is included in each kvpaxos
server. A kvpaxos server talks to its local Paxos peer (**via method calls**).
All kvpaxos replicas should stay identical; the only exception is that some
replicas may lag others if they are not reachable. If a replica isn't reachable
for a while, but then starts being reachable, it should eventually catch up.
