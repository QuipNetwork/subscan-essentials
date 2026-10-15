package service

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	substrate "github.com/itering/substrate-api-rpc"
	rpcmodel "github.com/itering/substrate-api-rpc/model"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/blake2b"
)

func babeFixture(t *testing.T, name string) []byte {
	t.Helper()
	root := filepath.Join("..", "..", "testdata", "quip-babe")
	data, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	require.NoError(t, err)
	var manifest struct {
		SDKCommit string            `json:"sdkCommit"`
		Files     map[string]string `json:"files"`
	}
	require.NoError(t, json.Unmarshal(data, &manifest))
	require.Equal(t, "845eb9986c7131056b077d14ad7249f9d65ca111", manifest.SDKCommit)
	raw, err := os.ReadFile(filepath.Join(root, name))
	require.NoError(t, err)
	require.Equal(t, manifest.Files[name], hex.EncodeToString(sha256Sum(raw)))
	return raw
}

func sha256Sum(data []byte) []byte { sum := sha256.Sum256(data); return sum[:] }

func TestSDKGeneratedHybridBabeAuthor(t *testing.T) {
	validators := []string{strings.Repeat("aa", 32), strings.Repeat("bb", 32)}
	for _, name := range []string{"primary.scale", "secondary-vrf.scale"} {
		t.Run(name, func(t *testing.T) {
			data := babeFixture(t, name)
			require.Len(t, data, 840)
			wrap := func(raw []byte) []byte {
				encoded, err := json.Marshal(substrate.PreRuntime{Engine: substrate.CidBabe, Data: "0x" + hex.EncodeToString(raw)})
				require.NoError(t, err)
				return encoded
			}
			author, err := authorFromDigest(wrap(data), validators)
			require.NoError(t, err)
			require.Equal(t, validators[1], author)
			for _, malformed := range [][]byte{nil, data[:12], data[:109], data[:839], append(append([]byte{}, data...), 0)} {
				_, err := authorFromDigest(wrap(malformed), validators)
				require.Error(t, err)
			}
			bad := append([]byte{}, data...)
			bad[len(bad)-1] = 1
			_, err = authorFromDigest(wrap(bad), validators)
			require.ErrorContains(t, err, "padding")
			bad = append([]byte{}, data...)
			bad[0] = 4
			_, err = authorFromDigest(wrap(bad), validators)
			require.Error(t, err)
			_, err = authorFromDigest(wrap(data), validators[:1])
			require.ErrorContains(t, err, "author index")
		})
	}
}

func TestNativeRuntimeSessionTransition(t *testing.T) {
	var transition struct {
		RuntimeCommit      string   `json:"runtimeCommit"`
		ParentSessionIndex uint     `json:"parentSessionIndex"`
		PostSessionIndex   uint     `json:"postSessionIndex"`
		ParentValidators   []string `json:"parentValidators"`
		PostValidators     []string `json:"postValidators"`
		Digest             string   `json:"digest"`
		ExpectedAuthor     string   `json:"expectedAuthor"`
	}
	require.NoError(t, json.Unmarshal(babeFixture(t, "session-transition.json"), &transition))
	require.Equal(t, "52a9070944d4cd2319a5cddcd2da1d8eb0a63abc", transition.RuntimeCommit)
	require.Zero(t, transition.ParentSessionIndex)
	require.Equal(t, uint(1), transition.PostSessionIndex)
	raw, err := json.Marshal(substrate.PreRuntime{Engine: substrate.CidBabe, Data: transition.Digest})
	require.NoError(t, err)
	author, err := authorFromDigest(raw, transition.PostValidators)
	require.NoError(t, err)
	require.Equal(t, transition.ExpectedAuthor, author)
	wrong, err := authorFromDigest(raw, transition.ParentValidators)
	require.NoError(t, err)
	require.NotEqual(t, transition.ExpectedAuthor, wrong)
}

func TestArchivedPrimaryBabeHeader(t *testing.T) {
	var provenance struct {
		BlockNumber      uint32 `json:"blockNumber"`
		BlockHash        string `json:"blockHash"`
		SourceFileSHA256 string `json:"sourceFileSHA256"`
		AuthorityIndex   int    `json:"authorityIndex"`
	}
	require.NoError(t, json.Unmarshal(babeFixture(t, "aglais-591853-provenance.json"), &provenance))
	raw := babeFixture(t, "aglais-591853.json")
	require.Equal(t, provenance.SourceFileSHA256, hex.EncodeToString(sha256Sum(raw)))
	var block rpcmodel.BlockResult
	require.NoError(t, json.Unmarshal(raw, &block))
	header := block.Block.Header
	number, err := strconv.ParseUint(strings.TrimPrefix(header.Number, "0x"), 16, 32)
	require.NoError(t, err)
	require.Equal(t, uint32(591853), provenance.BlockNumber)
	require.EqualValues(t, provenance.BlockNumber, number)
	decodeHex := func(s string) []byte {
		value, err := hex.DecodeString(strings.TrimPrefix(s, "0x"))
		require.NoError(t, err)
		return value
	}
	// Independently reproduce this header's SCALE encoding and Blake2b-256.
	encoded := decodeHex(header.ParentHash)
	require.True(t, number >= 1<<14 && number < 1<<30)
	encoded = binary.LittleEndian.AppendUint32(encoded, uint32(number)<<2|2)
	encoded = append(encoded, decodeHex(header.StateRoot)...)
	encoded = append(encoded, decodeHex(header.ExtrinsicsRoot)...)
	require.Less(t, len(header.Digest.Logs), 64)
	encoded = append(encoded, byte(len(header.Digest.Logs)<<2))
	for _, log := range header.Digest.Logs {
		encoded = append(encoded, decodeHex(log)...)
	}
	hash := blake2b.Sum256(encoded)
	require.Equal(t, "0xe5e55f20230e3e8f730ef47e54cd265e1aa9b1271cd506131231108d5695caa9", provenance.BlockHash)
	require.Equal(t, provenance.BlockHash, "0x"+hex.EncodeToString(hash[:]))
	logs, err := substrate.DecodeLogDigest(header.Digest.Logs)
	require.NoError(t, err)
	preRuntimeCount := 0
	for _, log := range logs {
		if !strings.EqualFold(log.Type, "PreRuntime") {
			continue
		}
		preRuntimeCount++
		data, err := json.Marshal(log.Value)
		require.NoError(t, err)
		var digest substrate.PreRuntime
		require.NoError(t, json.Unmarshal(data, &digest))
		payload := decodeHex(digest.Data)
		require.Len(t, payload, 840)
		require.Equal(t, byte(1), payload[0])
		require.Equal(t, 2, provenance.AuthorityIndex)
		require.EqualValues(t, provenance.AuthorityIndex, binary.LittleEndian.Uint32(payload[1:5]))
		// Synthetic labels isolate index mapping here. The separate captured
		// enrichment test uses Session.Validators recorded at this exact hash.
		validators := []string{strings.Repeat("11", 32), strings.Repeat("22", 32), strings.Repeat("33", 32)}
		author, err := authorFromDigest(data, validators)
		require.NoError(t, err)
		require.Equal(t, validators[provenance.AuthorityIndex], author)
	}
	require.Equal(t, 1, preRuntimeCount)
}
