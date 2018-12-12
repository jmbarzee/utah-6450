package raftkv

type Status string

const (
	StatusPending Status = "Pending"
	StatusNotLead Status = "NotLeader"
	StatusSuccess Status = "Success"
)

type (
	GetArgs struct {
		Key string
		ID  int64
	}
	GetReply struct {
		Term   int
		Status Status

		Value string
	}
)

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	var isLeader bool
	reply.Term, isLeader = kv.rf.GetState()
	if !isLeader {
		// Not the leader
		reply.Status = StatusNotLead
		kv.Lock()
		{
			op, ok := kv.Commited[args.ID]
			if !ok {
				// Not Started by me
			} else if op.ID != args.ID {
				// Started by me
			} else {
				// Completed
				reply.Status = StatusSuccess
				reply.Value = op.Value
			}
		}
		kv.Unlock()
		return
	} else {
		kv.Lock()
		{
			op, ok := kv.Commited[args.ID]
			if !ok {
				// Not Started
				kv.Commited[args.ID] = Op{}
				newOp := Op{
					Method: MethodGet,
					Key:    args.Key,
					ID:     args.ID,
				}
				_, reply.Term, isLeader = kv.rf.Start(newOp)
				if !isLeader {
					reply.Status = StatusPending
				}
			} else if op.ID != args.ID {
				// Started
				reply.Term, isLeader = kv.rf.GetState()
				if !isLeader {
					reply.Status = StatusPending
				}
			} else {
				// Completed
				reply.Status = StatusSuccess
				reply.Value = op.Value
			}
		}
		kv.Unlock()
		return
	}
}

// Put or Append
type (
	PutAppendArgs struct {
		Key    string
		Value  string
		Method Method
		ID     int64
	}
	PutAppendReply struct {
		Term   int
		Status Status
	}
)

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	var isLeader bool
	reply.Term, isLeader = kv.rf.GetState()
	if !isLeader {
		// Not the leader
		reply.Status = StatusNotLead
		kv.Lock()
		{
			op, ok := kv.Commited[args.ID]
			if !ok {
				// Not Started by me
			} else if op.ID != args.ID {
				// Started by me
			} else {
				// Completed
				reply.Status = StatusSuccess
			}
		}
		kv.Unlock()
		return
	} else {
		kv.Lock()
		{
			op, ok := kv.Commited[args.ID]
			if !ok {
				// Not Started
				kv.Commited[args.ID] = Op{}
				newOp := Op{
					Method: args.Method,
					Key:    args.Key,
					Value:  args.Value,
					ID:     args.ID,
				}
				_, reply.Term, isLeader = kv.rf.Start(newOp)
				if !isLeader {
					reply.Status = StatusPending
				}
			} else if op.ID != args.ID {
				// Started
				reply.Term, isLeader = kv.rf.GetState()
				if !isLeader {
					reply.Status = StatusPending
				}
			} else {
				// Completed
				reply.Status = StatusSuccess
			}
		}
		kv.Unlock()
		return
	}
}
