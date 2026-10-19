package dao

import (
	"context"
	"fmt"

	"github.com/itering/subscan-plugin/storage"
	"github.com/itering/subscan/model"
	bModel "github.com/itering/subscan/plugins/balance/model"
	"github.com/itering/subscan/util/address"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func GetAccountListCursor(db storage.DB, limit int, before, after *uint) ([]bModel.Account, bool, bool) {
	var accounts []bModel.Account
	d := db.GetDbInstance().(*gorm.DB)
	fetch := limit + 1
	var hasPrev, hasNext bool
	q := d.Model(bModel.Account{})
	if after != nil && *after > 0 {
		q = q.Where("id < ?", *after).Order("id desc")
	} else if before != nil && *before > 0 {
		q = q.Where("id > ?", *before).Order("id asc")
	} else {
		q = q.Order("id desc")
	}
	q = q.Limit(fetch).Find(&accounts)
	if q.Error != nil {
		return nil, false, false
	}
	if before != nil && *before > 0 {
		hasPrev = len(accounts) > limit
		if hasPrev {
			accounts = accounts[:limit]
		}
		for i, j := 0, len(accounts)-1; i < j; i, j = i+1, j-1 {
			accounts[i], accounts[j] = accounts[j], accounts[i]
		}
		hasNext = true
	} else {
		hasNext = len(accounts) > limit
		if hasNext {
			accounts = accounts[:limit]
		}
		hasPrev = after != nil && *after > 0
	}
	return accounts, hasPrev, hasNext
}

func GetAccountByAddress(ctx context.Context, db storage.DB, address string) *bModel.Account {
	var account bModel.Account
	d := db.GetDbInstance().(*gorm.DB)
	q := d.WithContext(ctx).Where("address = ?", address).First(&account)
	if q.Error != nil {
		return nil
	}
	return &account

}

func RefreshAccount(ctx context.Context, s *Storage, block *storage.Block, accountID string) error {
	accountID = address.Format(accountID)
	if accountID == "" {
		return nil
	}
	observation := ReadAccountState(ctx, block, accountID)
	return PersistAccountState(ctx, s, accountID, observation)
}

// PersistAccountState stores the historical observation and updates the latest
// view atomically. No RPC is performed while the SQL/count-row lock is held.
// A newer unknown observation clears stale amounts; an older retry never rolls
// back the view. At the same hash, known state cannot be downgraded by a failure.
func PersistAccountState(ctx context.Context, s *Storage, accountID string, state bModel.AccountState) error {
	if state.StateStatus != model.StateAvailable {
		state.Nonce, state.Balance, state.Locked, state.Reserved = nil, nil, nil, nil
	}
	err := withCounts(ctx, s, func(tx *gorm.DB, counts *bModel.ProjectionCounts) error {
		account := bModel.Account{Address: accountID}
		q := tx.Scopes(model.IgnoreDuplicate).Where("address = ?", accountID).FirstOrCreate(&account)
		if q.Error != nil {
			return q.Error
		}
		counts.Accounts += q.RowsAffected
		observation := bModel.AccountObservation{Address: accountID, ObservationHash: state.StateHash, AccountState: state}
		// The count lock serializes writers, including retries of the same account.
		var prior bModel.AccountObservation
		q = tx.Where("address = ? AND observation_hash = ?", accountID, state.StateHash).Limit(1).Find(&prior)
		if q.Error != nil {
			return q.Error
		}
		if q.RowsAffected > 0 && prior.StateStatus != model.StateUnknown && state.StateStatus == model.StateUnknown {
			state = prior.AccountState
		} else {
			if err := tx.Clauses(clause.OnConflict{UpdateAll: true}).Create(&observation).Error; err != nil {
				return err
			}
		}
		if account.StateBlockNum != nil && (state.StateBlockNum == nil || *account.StateBlockNum > *state.StateBlockNum) {
			return nil
		}
		if account.StateBlockNum != nil && state.StateBlockNum != nil && *account.StateBlockNum == *state.StateBlockNum && account.StateHash != state.StateHash {
			return fmt.Errorf("conflicting finalized account state at block %d", *state.StateBlockNum)
		}
		if account.StateHash == state.StateHash && account.StateStatus != "" && account.StateStatus != model.StateUnknown && state.StateStatus == model.StateUnknown {
			return nil
		}
		return tx.Model(&account).Updates(map[string]interface{}{
			"nonce": state.Nonce, "balance": state.Balance, "locked": state.Locked, "reserved": state.Reserved,
			"state_status": state.StateStatus, "state_hash": state.StateHash, "state_block_num": state.StateBlockNum,
			"state_spec_version": state.StateSpecVersion, "state_error": state.StateError,
		}).Error
	})
	if err != nil {
		return err
	}
	return SyncCounts(ctx, s)
}

func (s *Storage) AddOrUpdateItem(c context.Context, item interface{}, keys []string, updates ...string) *gorm.DB {
	var keyFields []clause.Column
	for _, key := range keys {
		keyFields = append(keyFields, clause.Column{Name: key})
	}
	db := s.Dao.GetDbInstance().(*gorm.DB)
	if len(updates) > 0 {
		return db.WithContext(c).Clauses(clause.OnConflict{
			Columns:   keyFields,
			DoUpdates: clause.AssignmentColumns(updates),
		}).Create(item)
	}
	return db.WithContext(c).Clauses(clause.OnConflict{
		Columns:   keyFields,
		UpdateAll: true,
	}).Create(item)
}
