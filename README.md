Dumb RDBMS for my study

# Done
* CREATE DATABASE, CREATE TABLE, INSERT, UPDATE, SELECT, INNER JOIN
* Persist to Disk (Wal and Snapshot)
* Transaction
* Multiple process (goroutine)
* Test

# TODO
* Replication
* Persist (Distribution, multiple write in one transaction)
* Multi-tenant (Send tenant-id with SQL and not read other tenant's data)
* Index
* Remove `panic`
* Abort ambiguous column identifier (SELECT id FROM a, b)
