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
	"github.com/ailabstw/go-pttai/common/types"
)

func (pm *BaseProtocolManager) handleDeleteMemberLog(oplog *BaseOplog, info *ProcessPersonInfo) ([]*BaseOplog, error) {

	obj := NewEmptyMember()
	pm.SetMemberObjDB(obj)

	opData := &MemberOpDeleteMember{}

	toBroadcastLogs, err := pm.HandleDeletePersonLog(oplog, obj, opData, types.StatusDeleted, pm.SetMemberDB, pm.postdeleteMember)
	if err != nil {
		return nil, err
	}

	return toBroadcastLogs, nil
}

func (pm *BaseProtocolManager) handlePendingDeleteMemberLog(oplog *BaseOplog, info *ProcessPersonInfo) (types.Bool, []*BaseOplog, error) {

	obj := NewEmptyMember()
	pm.SetMemberObjDB(obj)

	opData := &MemberOpDeleteMember{}

	return pm.HandlePendingDeletePersonLog(oplog, info, obj, opData, types.StatusInternalDeleted, types.StatusPendingDeleted, pm.SetMemberDB)
}

func (pm *BaseProtocolManager) setNewestDeleteMemberLog(oplog *BaseOplog) (types.Bool, error) {
	obj := NewEmptyMember()
	pm.SetMemberObjDB(obj)

	return pm.SetNewestDeletePersonLog(oplog, obj)
}

func (pm *BaseProtocolManager) handleFailedDeleteMemberLog(oplog *BaseOplog) error {
	obj := NewEmptyMember()
	pm.SetMemberObjDB(obj)

	return pm.HandleFailedDeletePersonLog(oplog, obj)
}

func (pm *BaseProtocolManager) handleFailedValidDeleteMemberLog(oplog *BaseOplog) error {
	return types.ErrNotImplemented
}
