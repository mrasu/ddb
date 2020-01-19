package server

import (
	"github.com/mrasu/ddb/server/data"
	"github.com/mrasu/ddb/server/pbs"
	"github.com/mrasu/ddb/server/structs"
)

func toPbCreateDatabase(c *structs.CreateDBChangeSet) *pbs.ChangeSet {
	return &pbs.ChangeSet{
		Lsn: c.Lsn,
		Data: &pbs.ChangeSet_CreateDB{CreateDB: &pbs.CreateDBChangeSet{
			Name: c.Name,
		}},
	}
}

func toPbCreateTable(c *structs.CreateTableChangeSet) *pbs.ChangeSet {
	return &pbs.ChangeSet{
		Lsn: c.Lsn,
		Data: &pbs.ChangeSet_CreateTable{CreateTable: &pbs.CreateTableChangeSet{
			DBName:   c.DBName,
			Name:     c.Name,
			RowMetas: data.ToPbRowMetas(c.RowMetas),
		}},
	}
}

func toPBInsertChangeSets(c *structs.InsertChangeSet) *pbs.ChangeSet {
	return &pbs.ChangeSet{
		Lsn: c.Lsn,
		Data: &pbs.ChangeSet_InsertSets{InsertSets: &pbs.InsertChangeSets{
			DBName:            c.DBName,
			TableName:         c.TableName,
			TransactionNumber: c.TransactionNumber,
			Rows: []*pbs.InsertRow{
				{Columns: c.Columns},
			},
		}},
	}
}

func toPBUpdateChangeSets(c *structs.UpdateChangeSet) *pbs.ChangeSet {
	return &pbs.ChangeSet{
		Lsn: c.Lsn,
		Data: &pbs.ChangeSet_UpdateSets{UpdateSets: &pbs.UpdateChangeSets{
			DBName:            c.DBName,
			TableName:         c.TableName,
			TransactionNumber: c.TransactionNumber,
			Rows: []*pbs.UpdateRow{{
				PrimaryKeyId: c.PrimaryKeyId,
				Columns:      c.Columns,
			}},
		}},
	}
}

func toPBBeginChangeSets(c *structs.BeginChangeSet) *pbs.ChangeSet {
	return &pbs.ChangeSet{
		Lsn: c.Lsn,
		Data: &pbs.ChangeSet_Begin{Begin: &pbs.BeginChangeSet{
			Number: c.Number,
		}},
	}
}

func toPBCommitChangeSets(c *structs.CommitChangeSet) *pbs.ChangeSet {
	return &pbs.ChangeSet{
		Lsn: c.Lsn,
		Data: &pbs.ChangeSet_Commit{Commit: &pbs.CommitChangeSet{
			Number: c.Number,
		}},
	}
}

func toPBRollbackChangeSets(c *structs.RollbackChangeSet) *pbs.ChangeSet {
	return &pbs.ChangeSet{
		Lsn: c.Lsn,
		Data: &pbs.ChangeSet_Rollback{Rollback: &pbs.RollbackChangeSet{
			Number: c.Number,
		}},
	}
}

func toPBAbortChangeSets(c *structs.AbortChangeSet) *pbs.ChangeSet {
	return &pbs.ChangeSet{
		Lsn: c.Lsn,
		Data: &pbs.ChangeSet_Abort{Abort: &pbs.AbortChangeSet{
			Number: c.Number,
		}},
	}
}

func toStructsChangeSets(pbcs *pbs.ChangeSet) []structs.ChangeSet {
	switch c := pbcs.Data.(type) {
	case *pbs.ChangeSet_CreateDB:
		return []structs.ChangeSet{&structs.CreateDBChangeSet{
			Lsn:  pbcs.Lsn,
			Name: c.CreateDB.Name,
		}}
	case *pbs.ChangeSet_CreateTable:
		return []structs.ChangeSet{&structs.CreateTableChangeSet{
			Lsn:      pbcs.Lsn,
			DBName:   c.CreateTable.DBName,
			Name:     c.CreateTable.Name,
			RowMetas: data.ToRowMetas(c.CreateTable.RowMetas),
		}}
	case *pbs.ChangeSet_InsertSets:
		var rows []structs.ChangeSet
		for _, r := range c.InsertSets.Rows {
			rows = append(rows, &structs.InsertChangeSet{
				Lsn:               pbcs.Lsn,
				DBName:            c.InsertSets.DBName,
				TableName:         c.InsertSets.TableName,
				Columns:           r.Columns,
				TransactionNumber: c.InsertSets.TransactionNumber,
			})
		}
		return rows
	case *pbs.ChangeSet_UpdateSets:
		var rows []structs.ChangeSet
		for _, r := range c.UpdateSets.Rows {
			rows = append(rows, &structs.UpdateChangeSet{
				Lsn:               pbcs.Lsn,
				DBName:            c.UpdateSets.DBName,
				TableName:         c.UpdateSets.TableName,
				PrimaryKeyId:      r.PrimaryKeyId,
				Columns:           r.Columns,
				TransactionNumber: c.UpdateSets.TransactionNumber,
			})
		}
		return rows
	case *pbs.ChangeSet_Begin:
		return []structs.ChangeSet{&structs.BeginChangeSet{
			Lsn:    pbcs.Lsn,
			Number: c.Begin.Number,
		}}
	case *pbs.ChangeSet_Commit:
		return []structs.ChangeSet{&structs.CommitChangeSet{
			Lsn:    pbcs.Lsn,
			Number: c.Commit.Number,
		}}
	case *pbs.ChangeSet_Rollback:
		return []structs.ChangeSet{&structs.RollbackChangeSet{
			Lsn:    pbcs.Lsn,
			Number: c.Rollback.Number,
		}}
	case *pbs.ChangeSet_Abort:
		return []structs.ChangeSet{&structs.AbortChangeSet{
			Lsn:    pbcs.Lsn,
			Number: c.Abort.Number,
		}}
	default:
		panic("Unsupported changeSet")
	}
}
