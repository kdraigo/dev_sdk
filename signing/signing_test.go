package signing

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"os"
	"testing"
)

// testdata/signature_v2.json is lib/auth's file. If this fails, the platform
// and the SDK sign differently.
func TestSign_SharedFixture(t *testing.T) {
	raw, err := os.ReadFile("testdata/signature_v2.json")
	if err != nil {
		t.Fatal(err)
	}
	var fx struct {
		SeedHex string `json:"seed_hex"`
		Vectors []struct {
			Name      string              `json:"name"`
			Method    string              `json:"method"`
			Path      string              `json:"path"`
			Query     map[string][]string `json:"query"`
			Timestamp string              `json:"timestamp"`
			Nonce     string              `json:"nonce"`
			Body      string              `json:"body"`
			Canonical string              `json:"canonical"`
			Signature string              `json:"signature"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(raw, &fx); err != nil {
		t.Fatal(err)
	}
	seed, _ := hex.DecodeString(fx.SeedHex)
	priv := ed25519.NewKeyFromSeed(seed)
	if len(fx.Vectors) == 0 {
		t.Fatal("no vectors")
	}
	for _, v := range fx.Vectors {
		q := url.Values(v.Query)
		if got := Canonical(v.Method, v.Path, q, v.Timestamp, v.Nonce, []byte(v.Body)); got != v.Canonical {
			t.Errorf("%s: canonical\n got %q\nwant %q", v.Name, got, v.Canonical)
		}
		if got := Sign(priv, v.Method, v.Path, q, v.Timestamp, v.Nonce, []byte(v.Body)); got != v.Signature {
			t.Errorf("%s: signature differs", v.Name)
		}
	}
}

func TestHeaders(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	q := url.Values{"id": {"s1"}}
	h, err := Headers(StyleBacktester, "k", hex.EncodeToString(priv), "GET", "/api/v1/dev/session/ws", q, nil)
	if err != nil {
		t.Fatal(err)
	}
	if h.Get("X-API-KEY") != "k" || len(h.Get("X-Nonce")) != 32 {
		t.Fatalf("headers %v", h)
	}
	sig, _ := hex.DecodeString(h.Get("X-Signature"))
	if !ed25519.Verify(pub, []byte(Canonical("GET", "/api/v1/dev/session/ws", q, h.Get("X-Timestamp"), h.Get("X-Nonce"), nil)), sig) {
		t.Fatal("signature does not verify")
	}
	if _, err := Headers(StyleStandard, "k", "zz", "GET", "/", nil, nil); err == nil {
		t.Fatal("a bad key must fail")
	}
}
