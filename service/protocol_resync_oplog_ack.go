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

func (pm *BaseProtocolManager) ResyncOplogAck(
	myNewKeys [][]byte,
	theirNewKeys [][]byte,
	peer *PttPeer,

	setDB func(oplog *BaseOplog),
	setNewestOplog func(log *BaseOplog) error,

	resyncAckMsg OpType,
) error {

	return pm.SyncOplogNewOplogs(myNewKeys, theirNewKeys, peer, setDB, setNewestOplog, nil, resyncAckMsg)
}

func (pm *BaseProtocolManager) HandleResyncOplogAck(
	dataBytes []byte,
	peer *PttPeer,

	setDB func(oplog *BaseOplog),
	handleFailedValidOplog func(oplog *BaseOplog) error,
	handleOplogs func(oplogs []*BaseOplog, peer *PttPeer, isUpdateSyncTime bool, isSkipExpireTS bool) error,

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

	myOplogs, err := getOplogsFromKeys(setDB, data.MyNewKeys)
	if err != nil {
		return err
	}

	err = HandleFailedOplogs(myOplogs, setDB, handleFailedValidOplog)
	if err != nil {
		return err
	}

	err = handleOplogs(data.Oplogs, peer, true, true)
	if err != nil {
		return err
	}

	return nil
}
