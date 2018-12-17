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
)

/*
ResyncOplog: (The requester) requests resync oplog.

    1. get the merkle node with MerkleLevelNow between startTS and endTS.
    2. send to the peer.
*/
func (pm *BaseProtocolManager) ResyncOplog(
	peer *PttPeer,

	merkle *Merkle,

	startTS types.Timestamp,
	endTS types.Timestamp,

	resyncMsg OpType,

) error {
	return pm.SyncOplogAck(startTS, endTS, merkle, resyncMsg, peer)
}

/*
HandleResyncOplog: (The receiver) receives resync-oplog.
*/
func (pm *BaseProtocolManager) HandleResyncOplog(
	dataBytes []byte,
	peer *PttPeer,

	merkle *Merkle,

	setDB func(oplog *BaseOplog),
	setNewestOplog func(log *BaseOplog) error,

	resyncAckMsg OpType,

) error {

	data := &SyncOplogAck{}
	err := json.Unmarshal(dataBytes, data)
	if err != nil {
		return err
	}

	myNodes, err := merkle.GetMerkleTreeListByLevel(MerkleTreeLevelNow, data.StartTS, data.EndTS)
	if err != nil {
		return err
	}

	myNewKeys, theirNewKeys, err := MergeMerkleNodeKeys(myNodes, data.Nodes)
	if err != nil {
		return err
	}

	return pm.ResyncOplogAck(myNewKeys, theirNewKeys, peer, setDB, setNewestOplog, resyncAckMsg)

}
