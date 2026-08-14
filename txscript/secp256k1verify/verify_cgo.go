// Copyright (c) 2024 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

//go:build cgo && !windows

package secp256k1verify

/*
#cgo CFLAGS: -I./libsecp256k1 -I./libsecp256k1/src
#cgo CFLAGS: -DNDEBUG

#include "./libsecp256k1/src/secp256k1.c"
#include "./libsecp256k1/src/modules/extrakeys/main_impl.h"
#include "./libsecp256k1/src/modules/schnorrsig/main_impl.h"
#include "./libsecp256k1/src/precomputed_ecmult.c"
#include "./libsecp256k1/src/precomputed_ecmult_gen.c"

static secp256k1_context *secp256k1verify_new_context(void) {
	return secp256k1_context_create(SECP256K1_CONTEXT_SIGN |
		SECP256K1_CONTEXT_VERIFY);
}

// secp256k1verify_ecdsa returns 1 iff the 64-byte compact (r||s) signature is
// valid for msg32 under the parsed pubkey. Any parse failure returns 0.
static int secp256k1verify_ecdsa(const secp256k1_context *ctx,
	const unsigned char *sig64, const unsigned char *msg32,
	const unsigned char *pubkey, size_t pubkeylen) {
	secp256k1_ecdsa_signature sig;
	secp256k1_pubkey pk;
	if (secp256k1_ecdsa_signature_parse_compact(ctx, &sig, sig64) == 0) {
		return 0;
	}
	if (secp256k1_ec_pubkey_parse(ctx, &pk, pubkey, pubkeylen) == 0) {
		return 0;
	}
	return secp256k1_ecdsa_verify(ctx, &sig, msg32, &pk);
}

// secp256k1verify_schnorr returns 1 iff the 64-byte BIP-340 signature is valid
// for msg32 under the 32-byte x-only pubkey. Any parse failure returns 0.
static int secp256k1verify_schnorr(const secp256k1_context *ctx,
	const unsigned char *sig64, const unsigned char *msg32,
	const unsigned char *pubkey32) {
	secp256k1_xonly_pubkey pk;
	if (secp256k1_xonly_pubkey_parse(ctx, &pk, pubkey32) == 0) {
		return 0;
	}
	return secp256k1_schnorrsig_verify(ctx, sig64, msg32, 32, &pk);
}
*/
import "C"

import (
	"unsafe"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/ecdsa"
	"github.com/btcsuite/btcd/btcec/v2/schnorr"
)

var secpCtx *C.secp256k1_context

func init() {
	secpCtx = C.secp256k1verify_new_context()
}

// VerifyECDSA verifies an ECDSA signature against the public key for the
// provided 32-byte hash.
//
// libsecp256k1 is used as a fast positive path: when it accepts a signature we
// return true immediately (it is the same vetted verifier Bitcoin Core uses).
// On any libsecp rejection we fall back to btcec, so the result is bit-for-bit
// identical to the pure-Go build for every input libsecp rejects. This makes
// the cgo path strictly faster on the common (valid) case without any
// consensus divergence risk relative to the historical pure-Go behavior.
func VerifyECDSA(sig *ecdsa.Signature, hash []byte, pub *btcec.PublicKey) bool {
	if sig == nil || pub == nil || len(hash) != 32 {
		return false
	}
	r := sig.R()
	s := sig.S()
	rb := r.Bytes()
	sb := s.Bytes()
	var rs [64]byte
	copy(rs[:32], rb[:])
	copy(rs[32:], sb[:])
	comp := pub.SerializeCompressed()
	if len(comp) == 0 {
		return sig.Verify(hash, pub)
	}
	if C.secp256k1verify_ecdsa(secpCtx,
		(*C.uchar)(unsafe.Pointer(&rs[0])),
		(*C.uchar)(unsafe.Pointer(&hash[0])),
		(*C.uchar)(unsafe.Pointer(&comp[0])),
		C.size_t(len(comp))) != 0 {
		return true
	}
	return sig.Verify(hash, pub)
}

// VerifySchnorr verifies a BIP-340 signature against the public key for the
// provided 32-byte hash. It uses the same fast-positive + btcec-fallback
// strategy as VerifyECDSA.
func VerifySchnorr(sig *schnorr.Signature, hash []byte, pub *btcec.PublicKey) bool {
	if sig == nil || pub == nil || len(hash) != 32 {
		return false
	}
	sigRaw := sig.Serialize()
	comp := pub.SerializeCompressed()
	if len(sigRaw) != 64 || len(comp) != 33 {
		return sig.Verify(hash, pub)
	}
	var xpub [32]byte
	copy(xpub[:], comp[1:33])
	if C.secp256k1verify_schnorr(secpCtx,
		(*C.uchar)(unsafe.Pointer(&sigRaw[0])),
		(*C.uchar)(unsafe.Pointer(&hash[0])),
		(*C.uchar)(unsafe.Pointer(&xpub[0]))) != 0 {
		return true
	}
	return sig.Verify(hash, pub)
}
