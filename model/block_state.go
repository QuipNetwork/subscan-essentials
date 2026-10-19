package model

const (
	StateAvailable = "available"
	StateUnknown   = "unknown"
	StateAbsent    = "absent"
)

// BlockStateInfo describes auxiliary post-state observations. Empty legacy
// statuses are unverified; neither a zero index nor an empty author proves data.
type BlockStateInfo struct {
	SessionIndex     *uint32 `json:"session_index"`
	SessionStatus    string  `json:"session_status" gorm:"size:24;default:unknown"`
	SessionStateHash string  `json:"session_state_hash" gorm:"size:100"`
	SessionError     string  `json:"session_error" gorm:"type:text"`
	AuthorStatus     string  `json:"author_status" gorm:"size:24;default:unknown"`
	AuthorStateHash  string  `json:"author_state_hash" gorm:"size:100"`
	AuthorError      string  `json:"author_error" gorm:"type:text"`
}

func (b *ChainBlock) StateInfo() BlockStateInfo {
	info := b.BlockStateInfo
	if info.SessionStatus == "" {
		info.SessionStatus = StateUnknown
	}
	if info.AuthorStatus == "" {
		info.AuthorStatus = StateUnknown
	}
	if info.SessionStatus != StateAvailable {
		info.SessionIndex = nil
	}
	return info
}
