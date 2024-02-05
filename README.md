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
for a while, but then starts being reachable, it should eventually catch up (
learn about operations that it missed).

## Test
To test your codes, try `go test -v` under the kvpaxos folder. You may see some 
error messages during the test, but as long as it shows "Passed" in the end 
of the test case, it passes the test case.

## Hints
Here's a plan for reference:

1. Fill in the Op struct in server.go with the "value" information that kvpaxos
   will use Paxos to agree on, for each client request. Op field names must
   start with capital letters. You should use Op structs as the agreed-on values
   -- for example, you should pass Op structs to Paxos Start(). Go's RPC can
   marshall/unmarshall Op structs; the call to gob.Register() in StartServer()
   teaches it how.
2. Implement the PutAppend() handler in server.go. It should enter a Put or
   Append Op in the Paxos log (i.e., use Paxos to allocate a Paxos instance,
   whose value includes the key and value (so that other kvpaxoses know about
   the Put() or Append())). An Append Paxos log entry should contain the
   Append's arguments, but not the resulting value, since the result might be
   large.
3. Implement a Get() handler. It should enter a Get Op in the Paxos log, and
   then "interpret" the the log before that point to make sure its key/value
   database reflects all recent Put()s.
4. Add code to cope with duplicate client requests, including situations where
   the client sends a request to one kvpaxos replica, times out waiting for a
   reply, and re-sends the request to a different replica. The client request
   should execute just once. Please make sure that your scheme for duplicate
   detection frees server memory quickly, for example by having the client tell
   the servers which RPCs it has heard a reply for. It's OK to piggyback this
   information on the next client request.

