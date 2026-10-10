// Package signing signs requests to the kdraigo platform with request
// signing version 2, a copy of lib/auth/signing_v2.go (dev_sdk does not
// import lib). The SDK signs with it; code that calls the platform directly
// can too:
//
//	KDRAIGO-SIG-V2\nMETHOD\nPATH\nQUERY\nTIMESTAMP\nNONCE\nhex(SHA256(BODY))
//
// Version 1 signed only METHOD\nPATH\nTIMESTAMP\nBODY: not the query string,
// and a captured signature could be sent again for five minutes. Version 2
// also signs the query and a one-time nonce, which the platform accepts
// once. testdata/signature_v2.json is lib's file; signing_test.go keeps the
// two in step.
package signing

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const tag = "KDRAIGO-SIG-V2"

// credentialParams are never part of the signed query.
var credentialParams = map[string]bool{"key_id": true, "signature": true, "timestamp": true, "nonce": true}

// Canonical is the string a version 2 signature covers.
func Canonical(method, path string, query url.Values, timestamp, nonce string, body []byte) string {
	signed := url.Values{}
	for k, vs := range query {
		if !credentialParams[k] {
			signed[k] = vs
		}
	}
	sum := sha256.Sum256(body)
	return strings.Join([]string{tag, method, path, signed.Encode(), timestamp, nonce, hex.EncodeToString(sum[:])}, "\n")
}

// Sign returns the hex version 2 signature of a request.
func Sign(priv ed25519.PrivateKey, method, path string, query url.Values, timestamp, nonce string, body []byte) string {
	return hex.EncodeToString(ed25519.Sign(priv, []byte(Canonical(method, path, query, timestamp, nonce, body))))
}

// NewNonce returns a fresh nonce: 16 random bytes in hex.
func NewNonce() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// HeaderStyle names the key-id header a service reads: backtester_engine
// says X-API-KEY, the others X-Key-ID.
type HeaderStyle int

const (
	StyleStandard HeaderStyle = iota
	StyleBacktester
)

// Headers signs a request now and returns its credential headers. path is
// the path the service receives (after any gateway prefix is stripped).
func Headers(style HeaderStyle, keyID, privateKeyHex, method, path string, query url.Values, body []byte) (http.Header, error) {
	priv, err := hex.DecodeString(privateKeyHex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode private key: %v", err)
	}
	if len(priv) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("invalid private key size: expected %d, got %d", ed25519.PrivateKeySize, len(priv))
	}
	nonce, err := NewNonce()
	if err != nil {
		return nil, err
	}
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	h := http.Header{}
	keyHeader := "X-Key-ID"
	if style == StyleBacktester {
		keyHeader = "X-API-KEY"
	}
	h.Set(keyHeader, keyID)
	h.Set("X-Signature", Sign(ed25519.PrivateKey(priv), method, path, query, ts, nonce, body))
	h.Set("X-Timestamp", ts)
	h.Set("X-Nonce", nonce)
	return h, nil
}
