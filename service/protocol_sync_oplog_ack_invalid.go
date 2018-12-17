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
	"bytes"
	"math/rand"
	"reflect"

	"github.com/ailabstw/go-pttai/common/types"
)

type SyncOplogAckInvalid struct{}

/*
SyncOplogAckInvalid: I (The receiver) Issuing that the sync-oplog is invalid (ValidateMerkleTree).

	1. check isToSyncPeer (The peer is considered as valid and I need to sync with them.)
	2. if isToSyncPeer: ResyncOplog (with resyncOp. I become the requester.)
	3. Send invalidOp to the peer (notify it that the oplog is invalid. the peer needs to do some action and resync the oplog.)
*/
func (pm *BaseProtocolManager) SyncOplogAckInvalid(
	peer *PttPeer,

	theirSyncTS types.Timestamp,
	mySyncTS types.Timestamp,

	merkle *Merkle,

	resyncOp OpType,
	refineResyncMsg OpType,
	invalidOp OpType,
) error {

	// my devices

	isToSyncPeer := pm.syncOplogAckInvalidIsToSyncPeer(peer, theirSyncTS, mySyncTS)

	if isToSyncPeer {
		return pm.RefineResyncOplog(
			peer,

			merkle,
			MerkleTreeLevelYear,
			types.ZeroTimestamp,
			mySyncTS,

			resyncOp,
			refineResyncMsg,
		)
	}

	pm.SendDataToPeer(invalidOp, &SyncOplogAckInvalid{}, peer)

	myID := pm.Ptt().GetMyEntity().GetID()
	if !pm.IsMaster(myID, false) {

		pm.UnregisterPeer(peer, false, false, false)
	}

	return nil
}

/*
syncOplogAckInvalidIsToSyncPeer checks whether I should sync with the peer.

	1. If the peer is just the member: not sync with the member.
	2. If I am not the master, and the peer is the master: sync with the member.
	3. (We are in the same level. peer as PeerTypeMe, or we both are the masters.)
	4. => cmp sync-ts.
	5. => cmp id.
*/
func (pm *BaseProtocolManager) syncOplogAckInvalidIsToSyncPeer(
	peer *PttPeer,
	theirSyncTS types.Timestamp,
	mySyncTS types.Timestamp,
) bool {

	ptt := pm.Ptt()
	myID := ptt.GetMyEntity().GetID()
	myNodeID := ptt.MyNodeID()

	isMe := peer.PeerType == PeerTypeMe
	isMeMaster := pm.IsMaster(myID, false)
	isPeerMaster := pm.IsMaster(peer.UserID, false)

	if !isMe && !isPeerMaster {
		return false
	}

	if !isMe && !isMeMaster && isPeerMaster {
		return true
	}

	if theirSyncTS.IsLess(mySyncTS) {
		return false
	}
	if mySyncTS.IsLess(theirSyncTS) {
		return true
	}

	// Me: follow the smallest node-id
	if isMe {
		peerNodeID := peer.GetID()
		if bytes.Compare(myNodeID[:], peerNodeID[:]) < 0 {
			return false
		}

		return true
	}

	// masters: follow the smallest master-id
	if bytes.Compare(myID[:], peer.UserID[:]) < 0 {
		return false
	}

	return true
}

/*
HandleSyncOplogAckInvalid: I (the requester) received the msg that my oplogs are invalid.

    1. if the peer is PeerTypeMe or the peer is master: do Resync
    2. if I am the master (asked from non-master): invalid op
    3. (The peer is not the master)
    3.1. if I am connecting to the master => get the master-peer and resync with the master.
    4. Try to connect to the master.
*/
func (pm *BaseProtocolManager) HandleSyncOplogAckInvalid(
	peer *PttPeer,

	merkle *Merkle,

	resyncOp OpType,
	refineResyncMsg OpType,
) error {

	myID := pm.Ptt().GetMyEntity().GetID()

	isMe := peer.PeerType == PeerTypeMe
	isMeMaster := pm.IsMaster(myID, false)
	isPeerMaster := pm.IsMaster(peer.UserID, false)

	// 0. get to-sync-time.
	toSyncTime, err := merkle.ToSyncTime()
	if err != nil {
		return err
	}

	// 1. the peer is PeerTypeMe or the peer is master
	if isMe || isPeerMaster {
		return pm.RefineResyncOplog(
			peer,

			merkle,
			MerkleTreeLevelYear,
			types.ZeroTimestamp,
			toSyncTime,

			resyncOp,
			refineResyncMsg,
		)
	}

	// 2. I am the master and the peer is not master.
	if isMeMaster {
		return ErrInvalidOp
	}

	// 3. I am connecting to the master.
	masterPeerList := pm.Peers().ImportantPeerList(false)
	lenMasterPeerList := len(masterPeerList)
	if lenMasterPeerList > 0 {
		randIdx := rand.Intn(lenMasterPeerList)
		masterPeer := masterPeerList[randIdx]
		return pm.RefineResyncOplog(
			masterPeer,

			merkle,
			MerkleTreeLevelYear,
			types.ZeroTimestamp,
			toSyncTime,

			resyncOp,
			refineResyncMsg,
		)
	}

	// 4. try to connect the master-node.
	masters := pm.masters
	pm.lockMaster.RLock()
	defer pm.lockMaster.RUnlock()

	if len(masters) == 0 {
		return ErrInvalidMaster0
	}

	var masterID *types.PttID
	for _, master := range masters {
		if !reflect.DeepEqual(myID, master.ID) {
			masterID = master.ID
			break
		}
	}

	if masterID == nil {
		return ErrInvalidMaster0
	}

	userNodeID, err := pm.Ptt().GetMyEntity().GetUserNodeID(masterID)
	if err != nil {
		return err
	}
	if userNodeID == nil {
		return ErrInvalidMaster0
	}

	opKey, err := pm.GetOldestOpKey(false)
	if err != nil {
		return err
	}

	pm.Ptt().AddDial(userNodeID, opKey.Hash, PeerTypeImportant)

	return nil
}
