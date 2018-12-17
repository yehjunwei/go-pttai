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

import "github.com/ailabstw/go-pttai/common/types"

type RefineResyncOplog struct {
	Nodes   []*MerkleNode   `json:"n"`
	Level   MerkleTreeLevel `json:"l"`
	StartTS types.Timestamp `json:"sTS"`
	EndTS   types.Timestamp `json:"eTS"`
}

/*
RefineResyncOplog: (The requester) requests refining resync oplog. Given the current level and the time-range. We would like to know the blocks that is not synced, and resync those blocks.

    1. if level is already MerkleTreeLevelNow: do resync.
    2. get the nodes between startTS and endTS.
    3. send to the peer.
*/
func (pm *BaseProtocolManager) RefineResyncOplog(
	peer *PttPeer,

	merkle *Merkle,
	level MerkleTreeLevel,
	startTS types.Timestamp,
	endTS types.Timestamp,

	resyncMsg OpType,
	refineResyncMsg OpType,

) error {

	if level == MerkleTreeLevelNow {
		return pm.ResyncOplog(peer, merkle, startTS, endTS, resyncMsg)
	}

	nodes, err := merkle.GetMerkleTreeListByLevel(level, startTS, endTS)
	if err != nil {
		return err
	}

	data := &RefineResyncOplog{
		Nodes:   nodes,
		Level:   level,
		StartTS: startTS,
		EndTS:   endTS,
	}

	return pm.SendDataToPeer(refineResyncMsg, data, peer)
}
