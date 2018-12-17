// Copyright 2018 The go-pttai Authors
// This file is part of the go-pttai library.
//
// The go-pttai library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-pttai library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-pttai library. If not, see <http://www.gnu.org/licenses/>.

package service

import (
	"encoding/json"

	"github.com/ailabstw/go-pttai/common/types"
	"github.com/ailabstw/go-pttai/log"
)

// sync-oplog
type SyncOplog struct {
	ToSyncTime  types.Timestamp `json:"LT"`
	ToSyncNodes []*MerkleNode   `json:"LN"`
}

/*
SyncOplog: I (The requester) initiate sync-oplog.

Expected merkle-tree-list length: 24 (hour) + 31 (day) + 12 (month) + n (year)
(should be within the packet-limit)
*/
func (pm *BaseProtocolManager) SyncOplog(peer *PttPeer, merkle *Merkle, op OpType) error {
	ptt := pm.Ptt()
	myInfo := ptt.GetMyEntity()
	if myInfo.GetStatus() != types.StatusAlive {
		log.Warn("SyncOplog: I am not alive", "status", myInfo.GetStatus())
		return nil
	}

	e := pm.Entity()
	if e.GetStatus() != types.StatusAlive {
		return nil
	}

	// 1. get to-sync-time.
	toSyncTime, err := merkle.ToSyncTime()
	if err != nil {
		return err
	}

	// 2. get to-sync-nodes
	toSyncNodes, _, err := merkle.GetMerkleTreeList(toSyncTime)
	if err != nil {
		return err
	}

	// 3. send to the peer.
	syncOplog := &SyncOplog{
		ToSyncTime:  toSyncTime,
		ToSyncNodes: toSyncNodes,
	}

	err = pm.SendDataToPeer(op, syncOplog, peer)
	if err != nil {
		return err
	}

	return nil
}

/*
HandleSyncOplog: I (The receiver) received sync-oplog. (MerkleTreeList should be within the packet-limit.)

	1. get my merkle-tree-list.
	2. validate merkle tree
	3. SyncOplogAck
*/
func (pm *BaseProtocolManager) HandleSyncOplog(
	dataBytes []byte,
	peer *PttPeer,
	merkle *Merkle,
	op OpType,
) error {
	ptt := pm.Ptt()
	myInfo := ptt.GetMyEntity()
	if myInfo.GetStatus() != types.StatusAlive {
		return nil
	}

	e := pm.Entity()
	if e.GetStatus() != types.StatusAlive {
		return nil
	}

	data := &SyncOplog{}
	err := json.Unmarshal(dataBytes, data)
	if err != nil {
		return err
	}

	myToSyncTime, err := merkle.ToSyncTime()
	if err != nil {
		return err
	}

	toSyncTime := myToSyncTime
	if data.ToSyncTime.IsLess(toSyncTime) {
		toSyncTime = data.ToSyncTime
	}

	// get my merkle-tree-list.
	myToSyncNodes, _, err := merkle.GetMerkleTreeList(toSyncTime)
	if err != nil {
		return err
	}

	// 2. validate merkle tree
	isValid := ValidateMerkleTree(myToSyncNodes, data.ToSyncNodes, toSyncTime)
	if !isValid {
		return ErrInvalidOplog
	}

	// 3. SyncOplogAck
	return pm.SyncOplogAck(toSyncTime, merkle, op, peer)
}
