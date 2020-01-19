proto:
	protoc -I=server/pbs --go_out=server/pbs server/pbs/raft.proto
