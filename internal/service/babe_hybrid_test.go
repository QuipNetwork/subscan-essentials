package service

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	substrate "github.com/itering/substrate-api-rpc"
	"github.com/stretchr/testify/require"
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
