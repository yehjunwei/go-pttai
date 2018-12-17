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

type SyncOplogNewOplogs struct {
	Oplogs    []*BaseOplog `json:"O"`
	MyNewKeys [][]byte     `json:"K"`
}

/*
SyncOplogNewOplogs: (The requester) sending SyncOplogNewOplogs.

	1. get theirNewLogs.
	2. if we do not need to sync new oplogs: do postsync.
	3. setNewestLog
	4. SendDataToPeer.
*/
func (pm *BaseProtocolManager) SyncOplogNewOplogs(
	syncOplogAck *SyncOplogAck,
	myNewKeys [][]byte,
	theirNewKeys [][]byte,
	peer *PttPeer,
	setDB func(oplog *BaseOplog),
	setNewestOplog func(log *BaseOplog) error,
	postsync func(peer *PttPeer) error,
	newLogsMsg OpType,
) error {

	if len(theirNewKeys) == 0 && len(myNewKeys) == 0 {
		if postsync != nil {
			return postsync(peer)
		}

		return nil
	}

	theirNewLogs, err := getOplogsFromKeys(setDB, theirNewKeys)
	if err != nil {
		return err
	}

	if len(theirNewLogs) == 0 && len(myNewKeys) == 0 {
		if postsync != nil {
			return postsync(peer)
		}

		return nil
	}

	if setNewestOplog != nil {
		for _, log := range theirNewLogs {
			setNewestOplog(log)
		}
	}

	data := &SyncOplogNewOplogs{
		Oplogs:    theirNewLogs,
		MyNewKeys: myNewKeys,
	}

	err = pm.SendDataToPeer(newLogsMsg, data, peer)
	if err != nil {
		return err
	}

	return nil
}

/*
HandleSyncOplogNewOplogs: (The receiver) receives SyncOplogNewOplogs

	1. filter the oplogs to be after toSyncTime.
	2. handleOplogs
	3. SyncOplogNewOplogsAck
*/
func (pm *BaseProtocolManager) HandleSyncOplogNewOplogs(
	dataBytes []byte,
	peer *PttPeer,
	setDB func(oplog *BaseOplog),
	handleOplogs func(oplogs []*BaseOplog, peer *PttPeer, isUpdateSyncTime bool) error,
	setNewestOplog func(log *BaseOplog) error,
	newLogsAckMsg OpType,
) error {

	ptt := pm.Ptt()
	myInfo := ptt.GetMyEntity()
	if myInfo.GetStatus() != types.StatusAlive {
		return nil
	}

	entity := pm.Entity()
	if entity.GetStatus() != types.StatusAlive {
		return nil
	}

	data := &SyncOplogNewOplogs{}
	err := json.Unmarshal(dataBytes, data)
	if err != nil {
		return err
	}

	err = handleOplogs(data.Oplogs, peer, true)
	if err != nil {
		return err
	}

	return pm.SyncOplogNewOplogsAck(data.MyNewKeys, peer, setDB, setNewestOplog, newLogsAckMsg)
}
