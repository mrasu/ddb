package server

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/coreos/etcd/raft/raftpb"

	"github.com/coreos/etcd/raft"
	"github.com/mrasu/ddb/server/pbs"
)

type RaftServer struct {
	server *Server

	node    raft.Node
	storage *raft.MemoryStorage
}

func StartRaftServer(server *Server, id uint64) *RaftServer {
	storage := raft.NewMemoryStorage()
	c := &raft.Config{
		ID:              id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
	}
	n := raft.StartNode(c, []raft.Peer{{ID: id}})
	rs := &RaftServer{
		server:  server,
		node:    n,
		storage: storage,
	}
	go rs.listen()
	return rs
}

func (rs *RaftServer) listen() {
	t := time.NewTicker(100 * time.Millisecond)
	for {
		select {
		case <-t.C:
			fmt.Print(".")
			rs.node.Tick()
		case rd := <-rs.node.Ready():
			fmt.Printf("Ready!: %v\n", rs.node.Status())
			s := rd.HardState
			fmt.Printf("Received HardrdState: Term=%d, Vote=%d, Commit=%d\n", s.Term, s.Vote, s.Commit)

			for _, en := range rd.Entries {
				fmt.Printf("Received entry: Term=%d, Index=%d, Data=%s\n", en.Term, en.Index, string(en.Data))
			}
			rs.storage.Append(rd.Entries)
			fmt.Println(rd.Snapshot)

			for _, msg := range rd.Messages {
				fmt.Printf("Send to: %v\n", msg.To)
			}

			if !raft.IsEmptySnap(rd.Snapshot) {
				fmt.Println("Taking Snapshot...")
			}

			for _, entry := range rd.CommittedEntries {
				if entry.Type == raftpb.EntryConfChange {
					fmt.Printf("Processing confChange entry. (Term=%v, Index=%v, Data=%v)\n", entry.Term, entry.Index, entry.Data)
					var cc raftpb.ConfChange
					cc.Unmarshal(entry.Data)
					rs.node.ApplyConfChange(cc)
				} else if entry.Type == raftpb.EntryNormal {
					if len(entry.Data) == 0 {
						fmt.Printf("Processing empty entry. (Term=%v, Index=%v, Data=%v)\n", entry.Term, entry.Index, entry.Data)
					} else {
						cs := &pbs.ChangeSet{}
						err := proto.Unmarshal(entry.Data, cs)
						if err != nil {
							panic(err)
						}
						if cs.Data == nil {
							panic("Invalid Data")
						}

						fmt.Printf("Processing normal entry. (Term=%v, Index=%v, Instance=%v)\n", entry.Term, entry.Index, cs.Data)
						err = rs.server.ApplyChangeSet(cs, true)
						if err != nil {
							panic(err)
						}
					}
				} else {
					fmt.Printf("Processing unknown entry. (Term=%v, Index=%v, Data=%v)\n", entry.Term, entry.Index, entry.Data)
				}
			}
			rs.node.Advance()
		}
	}
}

func (rs *RaftServer) Propose(ctx context.Context, data []byte) error {
	return rs.node.Propose(ctx, data)
}
