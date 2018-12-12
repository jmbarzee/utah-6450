package raftkv

import (
	"context"
	"strings"

	"../raft"
)

func (kv *RaftKV) watchCommits(ctx context.Context, applyCh chan raft.ApplyMsg) {
Loop:
	for {
		select {
		case applyMsg := <-applyCh:
			kv.Lock()
			{
				op, ok := applyMsg.Command.(Op)
				if !ok {
					// log.Printf("Command could not assert to expected type: *** %v *** %v ***", applyMsg, applyMsg.Command)
				}
				switch op.Method {
				case MethodGet:
					value, ok := kv.KVstore[op.Key]
					if !ok {
						value = ""
					}
					op.Value = value
					kv.Commited[op.ID] = op

				case MethodPut:
					kv.KVstore[op.Key] = op.Value
					kv.Commited[op.ID] = op

				case MethodAppend:
					value, ok := kv.KVstore[op.Key]
					if !ok {
						value = ""
					}
					if !strings.Contains(value, op.Value) {
						op.Value = value + op.Value
						kv.KVstore[op.Key] = op.Value
						kv.Commited[op.ID] = op
					} else {
						op = op
					}

				}
			}
			kv.Unlock()
		case <-ctx.Done():
			break Loop
		}
	}
}