package server

import (
	"context"
	"fmt"
	golog "log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/rs/zerolog/log"

	"github.com/golang/protobuf/proto"

	"github.com/coreos/etcd/raft/raftpb"

	"github.com/coreos/etcd/raft"
	"github.com/mrasu/ddb/server/pbs"
)

type RaftServer struct {
	id uint64

	server *Server

	node    raft.Node
	storage *raft.MemoryStorage

	messageChan    chan raftpb.Message
	confChangeChan chan *raftpb.ConfChange

	peers map[uint64]int
}

// TODO: use gRPC and remove this variable.
var globalRafts = map[int]*RaftServer{}

func StartRaftServer(server *Server, id uint64) *RaftServer {
	storage := raft.NewMemoryStorage()
	logger := &raft.DefaultLogger{Logger: golog.New(os.Stderr, fmt.Sprintf("[raft%d] ", id), golog.LstdFlags)}
	logger.EnableDebug()
	c := &raft.Config{
		ID:              id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
		Logger:          logger,
	}
	n := raft.StartNode(c, []raft.Peer{{ID: id}})
	rs := &RaftServer{
		id: id,

		server:  server,
		node:    n,
		storage: storage,

		messageChan:    make(chan raftpb.Message),
		confChangeChan: make(chan *raftpb.ConfChange),

		peers: map[uint64]int{},
	}
	go rs.startRaft()
	go rs.startListening()

	globalRafts[int(id)*-1] = rs
	return rs
}

func (rs *RaftServer) startRaft() {
	t := time.NewTicker(100 * time.Millisecond)
	for {
		select {
		case <-t.C:
			fmt.Print(".")
			rs.node.Tick()
		case rd := <-rs.node.Ready():
			// TODO: way to revert, term, index management
			rs.Printf("Ready!: %v\n", rs.node.Status())
			s := rd.HardState
			rs.Printf("Received HardrdState: Term=%d, Vote=%d, Commit=%d\n", s.Term, s.Vote, s.Commit)

			for _, en := range rd.Entries {
				rs.Printf("Received entry: Term=%d, Index=%d, Data= %s\n", en.Term, en.Index, strings.Replace(string(en.Data), "\n", "\\n", -1))
			}
			rs.storage.Append(rd.Entries)

			for _, msg := range rd.Messages {
				globalRaftId := rs.peers[msg.To]
				rs.SendEntries(globalRaftId, msg)
			}

			if !raft.IsEmptySnap(rd.Snapshot) {
				rs.Printf("Taking Snapshot...\n")
			}

			for _, entry := range rd.CommittedEntries {
				if entry.Type == raftpb.EntryConfChange {
					var cc raftpb.ConfChange
					cc.Unmarshal(entry.Data)
					rs.Printf("Processing confChange entry. (Term=%v, Index=%v, Data=%v)\n", entry.Term, entry.Index, cc)

					// TODO: record to snapshot
					rs.node.ApplyConfChange(cc)

					switch cc.Type {
					case raftpb.ConfChangeAddNode:
						if cc.NodeID != rs.id {
							err := rs.addPeer(cc.NodeID, string(cc.Context))
							if err != nil {
								panic(err)
							}
						}
					default:
						panic(fmt.Sprintf("Unsupported ConfChange Type: %v", cc.Type))
					}
				} else if entry.Type == raftpb.EntryNormal {
					if len(entry.Data) == 0 {
						rs.Printf("Processing empty entry. (Term=%v, Index=%v, Data=%v)\n", entry.Term, entry.Index, entry.Data)
					} else {
						cs := &pbs.ChangeSet{}
						err := proto.Unmarshal(entry.Data, cs)
						if err != nil {
							panic(err)
						}
						if cs.Data == nil {
							panic("Invalid Data")
						}

						rs.Printf("Processing normal entry. (Term=%v, Index=%v, Instance=%v)\n", entry.Term, entry.Index, cs.Data)
						err = rs.server.ApplyChangeSet(cs, true)
						if err != nil {
							panic(err)
						}
					}
				} else {
					rs.Printf("Processing unknown entry. (Term=%v, Index=%v, Data=%v)\n", entry.Term, entry.Index, entry.Data)
				}
			}
			rs.node.Advance()
		}
	}
}

func (rs *RaftServer) startListening() {
	for {
		select {
		case bs := <-rs.messageChan:
			rs.Printf("Receive msg\n")
			ctx := context.Background()
			ctx2, _ := context.WithTimeout(ctx, 1*time.Second)
			err := rs.node.Step(ctx2, bs)
			if err != nil {
				log.Error().Stack().Err(err).Msg("Failed to propose ChangeSet")
			}
		case cc := <-rs.confChangeChan:
			ctx := context.Background()
			ctx2, _ := context.WithTimeout(ctx, 1*time.Second)
			err := rs.ProposeConfChange(ctx2, cc)
			if err != nil {
				log.Error().Stack().Err(err).Msg("ProposeConfChange failed")
			}
		}
	}
}

func (rs *RaftServer) SendEntries(globalRaftId int, msg raftpb.Message) {
	rs.Printf("Sending to: %d\n", globalRaftId)
	if r, ok := globalRafts[globalRaftId]; ok {
		// use `go` to emulate concurrency
		go func() {
			r.messageChan <- msg
		}()
	} else {
		panic("Invalid id for SendMessage.")
	}
}

func (rs *RaftServer) AskJoin(globalRaftId int, cc *raftpb.ConfChange) {
	rs.Printf("Asking join...\n")
	if r, ok := globalRafts[globalRaftId]; ok {
		// use `go` to emulate concurrency
		go func() {
			r.confChangeChan <- cc
		}()
		err := rs.addPeer(r.id, strconv.Itoa(globalRaftId))
		if err != nil {
			panic(err)
		}
	} else {
		panic("Invalid id for AskJoin.")
	}
}

func (rs *RaftServer) addPeer(id uint64, idStr string) error {
	peerId, err := strconv.Atoi(idStr)
	if err != nil {
		return err
	}

	if _, ok := globalRafts[peerId]; !ok {
		return errors.Errorf("Invalid peerId: %d", peerId)
	}

	if grId, ok := rs.peers[id]; ok {
		if grId == peerId {
			return nil
		}
		return errors.Errorf("Existing peerId: %d", id)
	}

	rs.Printf("Peer added!\n")
	rs.peers[id] = peerId
	return nil
}

func (rs *RaftServer) Propose(cs *pbs.ChangeSet) error {
	rs.Printf("Proposing %v\n", cs)
	out, err := proto.Marshal(cs)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	ctx2, _ := context.WithTimeout(ctx, 1*time.Second)
	return rs.node.Propose(ctx2, out)
}

func (rs *RaftServer) ProposeConfChange(ctx context.Context, cc *raftpb.ConfChange) error {
	return rs.node.ProposeConfChange(ctx, *cc)
}

func (rs *RaftServer) Printf(format string, a ...interface{}) {
	b := []interface{}{rs.id}
	for _, aa := range a {
		b = append(b, aa)
	}
	fmt.Printf("[%d] "+format, b...)
}

func (rs *RaftServer) InspectServer() {
	fmt.Printf("%d**************\n", rs.id)
	rs.server.Inspect()
}
