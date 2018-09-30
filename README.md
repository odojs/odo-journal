# Odo Journal

Distributed, p2p, append-only event streaming journal

Relies on leveldown style storage

# Generate self signed key
openssl genrsa -out key.pem 4096
openssl req -new -key key.pem -out csr.pem
openssl x509 -req -days 365 -in csr.pem -signkey key.pem -out cert.pem
