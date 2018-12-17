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

package me

import pkgservice "github.com/ailabstw/go-pttai/service"

/**********
 * AddMeOplog
 **********/

func (pm *ProtocolManager) HandleAddMeOplog(dataBytes []byte, peer *pkgservice.PttPeer) error {
	return pm.HandleAddOplog(dataBytes, pm.HandleMeOplogs, peer)
}

func (pm *ProtocolManager) HandleAddMeOplogs(dataBytes []byte, peer *pkgservice.PttPeer) error {
	return pm.HandleAddOplogs(dataBytes, pm.HandleMeOplogs, peer)
}

func (pm *ProtocolManager) HandleAddPendingMeOplog(dataBytes []byte, peer *pkgservice.PttPeer) error {
	return pm.HandleAddPendingOplog(dataBytes, pm.HandlePendingMeOplogs, peer)
}

func (pm *ProtocolManager) HandleAddPendingMeOplogs(dataBytes []byte, peer *pkgservice.PttPeer) error {
	return pm.HandleAddPendingOplogs(dataBytes, pm.HandlePendingMeOplogs, peer)
}

/**********
 * SyncMeOplog
 **********/

func (pm *ProtocolManager) HandleSyncMeOplog(dataBytes []byte, peer *pkgservice.PttPeer) error {
	return pm.HandleSyncOplog(dataBytes, peer, pm.meOplogMerkle, SyncMeOplogAckMsg)
}

func (pm *ProtocolManager) HandleSyncMeOplogAck(dataBytes []byte, peer *pkgservice.PttPeer) error {
	return pm.HandleSyncOplogAck(dataBytes, peer, pm.meOplogMerkle, pm.SetMeDB, pm.SetNewestMeOplog, pm.postsyncMeOplogs, SyncMeOplogNewOplogsMsg)
}

func (pm *ProtocolManager) HandleSyncNewMeOplog(dataBytes []byte, peer *pkgservice.PttPeer) error {
	return pm.HandleSyncOplogNewOplogs(dataBytes, peer, pm.SetMeDB, pm.HandleMeOplogs, pm.SetNewestMeOplog, SyncMeOplogNewOplogsAckMsg)
}

func (pm *ProtocolManager) HandleSyncNewMeOplogAck(dataBytes []byte, peer *pkgservice.PttPeer) error {
	return pm.HandleSyncOplogNewOplogsAck(dataBytes, peer, pm.SetMeDB, pm.HandleMeOplogs, pm.postsyncMeOplogs)
}

/**********
 * SyncPendingMeOplog
 **********/

func (pm *ProtocolManager) HandleSyncPendingMeOplog(dataBytes []byte, peer *pkgservice.PttPeer) error {
	return pm.HandleSyncPendingOplog(dataBytes, peer, pm.HandlePendingMeOplogs, pm.SetMeDB, pm.HandleFailedMeOplog, SyncPendingMeOplogAckMsg)
}

func (pm *ProtocolManager) HandleSyncPendingMeOplogAck(dataBytes []byte, peer *pkgservice.PttPeer) error {
	return pm.HandleSyncPendingOplogAck(dataBytes, peer, pm.HandlePendingMeOplogs)
}

/**********
 * HandleOplogs
 **********/

func (pm *ProtocolManager) HandleMeOplogs(oplogs []*pkgservice.BaseOplog, peer *pkgservice.PttPeer, isUpdateSyncTime bool, isSkipExpireTS bool) error {

	info := NewProcessMeInfo()
	merkle := pm.meOplogMerkle

	return pkgservice.HandleOplogs(
		oplogs,
		peer,
		isUpdateSyncTime,
		isSkipExpireTS,

		info,
		merkle,

		pm.SetMeDB,
		pm.processMeLog,
		pm.postprocessMeOplogs,
	)
}

func (pm *ProtocolManager) HandlePendingMeOplogs(oplogs []*pkgservice.BaseOplog, peer *pkgservice.PttPeer) error {

	info := NewProcessMeInfo()

	return pkgservice.HandlePendingOplogs(oplogs, peer, pm, info, pm.SetMeDB, pm.processPendingMeLog, pm.processMeLog, pm.postprocessMeOplogs)

}
