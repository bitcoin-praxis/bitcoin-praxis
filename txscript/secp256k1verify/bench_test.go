// Copyright (c) 2024 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package secp256k1verify

import (
	"testing"

	"github.com/btcsuite/btcd/btcec/v2/ecdsa"
	"github.com/btcsuite/btcd/btcec/v2/schnorr"
	decrec "github.com/decred/dcrd/dcrec/secp256k1/v4"
)

func mustGenKey(tb testing.TB) (*decrec.PrivateKey, *decrec.PublicKey) {
	tb.Helper()
	priv, err := decrec.GeneratePrivateKey()
	if err != nil {
		tb.Fatalf("gen key: %v", err)
	}
	return priv, priv.PubKey()
}

func genECDSAInputs(tb testing.TB, n int) ([]*ecdsa.Signature, [][]byte, []*decrec.PublicKey) {
	tb.Helper()
	sigs := make([]*ecdsa.Signature, n)
	hashes := make([][]byte, n)
	pubs := make([]*decrec.PublicKey, n)
	for i := 0; i < n; i++ {
		priv, pub := mustGenKey(tb)
		h := randHash(tb)
		sigs[i] = ecdsa.Sign(priv, h)
		hashes[i] = h
		pubs[i] = pub
	}
	return sigs, hashes, pubs
}

func genSchnorrInputs(tb testing.TB, n int) ([]*schnorr.Signature, [][]byte, []*decrec.PublicKey) {
	tb.Helper()
	sigs := make([]*schnorr.Signature, n)
	hashes := make([][]byte, n)
	pubs := make([]*decrec.PublicKey, n)
	for i := 0; i < n; i++ {
		priv, pub := mustGenKey(tb)
		h := randHash(tb)
		sig, err := schnorr.Sign(priv, h)
		if err != nil {
			tb.Fatalf("schnorr sign: %v", err)
		}
		sigs[i] = sig
		hashes[i] = h
		pubs[i] = pub
	}
	return sigs, hashes, pubs
}

// BenchmarkVerifyECDSA measures the secp256k1verify backend (libsecp256k1
// under CGo, btcec otherwise) for ECDSA verification.
func BenchmarkVerifyECDSA(b *testing.B) {
	sigs, hashes, pubs := genECDSAInputs(b, 256)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		x := i % len(sigs)
		if !VerifyECDSA(sigs[x], hashes[x], pubs[x]) {
			b.Fatal("verify false")
		}
	}
}

// BenchmarkVerifyECDSABtcec measures btcec's pure-Go ECDSA verification, the
// historical baseline. Compare against BenchmarkVerifyECDSA to see the
// backend speedup (under CGo, libsecp256k1 is ~4x faster).
func BenchmarkVerifyECDSABtcec(b *testing.B) {
	sigs, hashes, pubs := genECDSAInputs(b, 256)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		x := i % len(sigs)
		if !sigs[x].Verify(hashes[x], pubs[x]) {
			b.Fatal("verify false")
		}
	}
}

func BenchmarkVerifySchnorr(b *testing.B) {
	sigs, hashes, pubs := genSchnorrInputs(b, 256)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		x := i % len(sigs)
		if !VerifySchnorr(sigs[x], hashes[x], pubs[x]) {
			b.Fatal("verify false")
		}
	}
}

func BenchmarkVerifySchnorrBtcec(b *testing.B) {
	sigs, hashes, pubs := genSchnorrInputs(b, 256)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		x := i % len(sigs)
		if !sigs[x].Verify(hashes[x], pubs[x]) {
			b.Fatal("verify false")
		}
	}
}
