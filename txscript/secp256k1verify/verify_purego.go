// Copyright (c) 2024 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

//go:build !cgo || windows

// Package secp256k1verify provides a single signature-verification entry
// point used by the script interpreter. When CGo is available on a non-Windows
// platform, verification is backed by libsecp256k1 (see verify_cgo.go). This
// file is the pure-Go fallback used when CGo is disabled or on Windows, and
// simply delegates to btcec, preserving the historical pure-Go behavior
// exactly.
package secp256k1verify

import (
	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/ecdsa"
	"github.com/btcsuite/btcd/btcec/v2/schnorr"
)

// VerifyECDSA verifies an ECDSA signature against the public key for the
// provided hash. It delegates to btcec.
func VerifyECDSA(sig *ecdsa.Signature, hash []byte, pub *btcec.PublicKey) bool {
	if sig == nil || pub == nil {
		return false
	}
	return sig.Verify(hash, pub)
}

// VerifySchnorr verifies a BIP-340 signature against the public key for the
// provided hash. It delegates to btcec.
func VerifySchnorr(sig *schnorr.Signature, hash []byte, pub *btcec.PublicKey) bool {
	if sig == nil || pub == nil {
		return false
	}
	return sig.Verify(hash, pub)
}
