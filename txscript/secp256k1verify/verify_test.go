// Copyright (c) 2024 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package secp256k1verify

import (
	"crypto/rand"
	"testing"

	"github.com/btcsuite/btcd/btcec/v2/ecdsa"
	"github.com/btcsuite/btcd/btcec/v2/schnorr"
	decrec "github.com/decred/dcrd/dcrec/secp256k1/v4"
)

func randHash(tb testing.TB) []byte {
	tb.Helper()
	var h [32]byte
	if _, err := rand.Read(h[:]); err != nil {
		tb.Fatalf("rand: %v", err)
	}
	return h[:]
}

// TestVerifyECDSAMatchesBtcec is the differential consensus gate for ECDSA:
// for every input (valid and tampered), the secp256k1verify backend must
// return the same verdict as btcec's own verifier. Under CGo this compares
// libsecp256k1 against btcec on identical inputs; without CGo the backend
// delegates to btcec so the assertion is trivially true.
func TestVerifyECDSAMatchesBtcec(t *testing.T) {
	const n = 250
	for i := 0; i < n; i++ {
		priv, err := decrec.GeneratePrivateKey()
		if err != nil {
			t.Fatalf("gen key: %v", err)
		}
		pub := priv.PubKey()
		hash := randHash(t)
		sig := ecdsa.Sign(priv, hash)

		other, err := decrec.GeneratePrivateKey()
		if err != nil {
			t.Fatalf("gen key: %v", err)
		}
		wrongPub := other.PubKey()

		badHash := append([]byte(nil), hash...)
		badHash[0] ^= 1

		cases := []struct {
			name string
			hash []byte
			pub  *decrec.PublicKey
		}{
			{"valid", hash, pub},
			{"wrong-pub", hash, wrongPub},
			{"bad-hash", badHash, pub},
		}
		for _, c := range cases {
			got := VerifyECDSA(sig, c.hash, c.pub)
			want := sig.Verify(c.hash, c.pub)
			if got != want {
				t.Fatalf("ecdsa %s: backend=%v btcec=%v", c.name, got, want)
			}
		}
	}
}

// TestVerifySchnorrMatchesBtcec is the differential consensus gate for
// BIP-340 Schnorr signatures.
func TestVerifySchnorrMatchesBtcec(t *testing.T) {
	const n = 250
	for i := 0; i < n; i++ {
		priv, err := decrec.GeneratePrivateKey()
		if err != nil {
			t.Fatalf("gen key: %v", err)
		}
		pub := priv.PubKey()
		hash := randHash(t)
		sig, err := schnorr.Sign(priv, hash)
		if err != nil {
			t.Fatalf("schnorr sign: %v", err)
		}

		other, err := decrec.GeneratePrivateKey()
		if err != nil {
			t.Fatalf("gen key: %v", err)
		}
		wrongPub := other.PubKey()

		badHash := append([]byte(nil), hash...)
		badHash[0] ^= 1

		cases := []struct {
			name string
			hash []byte
			pub  *decrec.PublicKey
		}{
			{"valid", hash, pub},
			{"wrong-pub", hash, wrongPub},
			{"bad-hash", badHash, pub},
		}
		for _, c := range cases {
			got := VerifySchnorr(sig, c.hash, c.pub)
			want := sig.Verify(c.hash, c.pub)
			if got != want {
				t.Fatalf("schnorr %s: backend=%v btcec=%v", c.name, got, want)
			}
		}
	}
}
