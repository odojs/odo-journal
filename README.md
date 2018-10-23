# Odo Journal

Distributed, p2p, append-only event streaming journal

# Todo

[ ] Upgrade to promises

Relies on leveldown style storage

# Generate self signed key
openssl genrsa -out key.pem 4096
openssl req -new -key key.pem -out csr.pem
openssl x509 -req -days 365 -in csr.pem -signkey key.pem -out cert.pem

## Discovery States

1. Outgoing
2. Incoming
3. Active (heartbeat)
4. Lifesupport (needs prooving)

## Discovery

1. Peers discovered through DNS and DHT that do not match an existing known peer's host and port have their state set to 'connecting' and a connection attempt is started.
2. A connection attempt starts with a TLS socket connection and is followed by introductions which includes asking for the peer's id.
3. If an attempted connection has a timeout or another error occurs the connection is closed and the peer's state is set to lifesupport.
4. If during introductions the peer's id matches an existing peer or the same id as the initiating peer the connection is closed and the peer's state is set to purgatory with an appropriate reason.
5. If everything goes well the peer's state is set to active and the peer becomes available for communication. The host, port and id are saved to peer history.
6. A heartbeat is sent to all peers in active state that have had no recent activity for a set amount of time.
7. If a peer has a communication failure or misses a heartbeat the connection is closed and the peer's state is set to lifesupport.
8. Using an exponential backoff timeout peers in lifesupport state have their state set to 'connecting' and a connection attempt is started.
9. After a set timeout peers in purgatory state are deleted.

## Synchronisation

1. Peers exchange their catalogue of device ids and revisions, including if they are the source of device id or are subscribed to the source
2. A peer requests a catchup by providing device ids and last known revisions
3. The server streams as separate payloads the the latest snapshot for each id and all events since
4. A peer requests to be subscribed to any new event for a device id by providing a last known revision
5. The server remembers subscriptions and will catch up subscribers to the latest revision before streaming any new events live
6. Any issue or lost connection will drop the state back to step 1
7. After set interval or event count a snapshot of the data is built and saved
8. After an interval or event count all events before a set snapshot are deleted
