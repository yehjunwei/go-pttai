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

func NewObjectWithOplog(obj Object, oplog *BaseOplog) {
	obj.SetVersion(types.CurrentVersion)
	obj.SetID(oplog.ID)
	obj.SetCreateTS(oplog.UpdateTS)
	obj.SetUpdateTS(types.ZeroTimestamp)
	obj.SetStatus(types.StatusInternalSync)
	obj.SetCreatorID(oplog.CreatorID)
	obj.SetUpdaterID(oplog.CreatorID)
	obj.SetLogID(oplog.ID)
	obj.SetEntityID(oplog.dbPrefixID)
}

func SetDeleteObjectWithOplog(obj Object, oplog *BaseOplog) {
	obj.SetStatus(types.StatusDeleted)
	obj.SetLogID(oplog.ID)
	obj.SetUpdateTS(oplog.UpdateTS)
	obj.SetUpdaterID(oplog.CreatorID)
	obj.RemoveMeta()
}