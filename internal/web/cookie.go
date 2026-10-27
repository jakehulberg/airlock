package web

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"time"
)

// Cookies seals values into encrypted, authenticated cookies (AES-GCM), so
// the browser can hold the session without a server-side store.
type Cookies struct {
	aead   cipher.AEAD
	secure bool
}

func NewCookies(key string, secure bool) (*Cookies, error) {
	if len(key) < 32 {
		return nil, errors.New("session key must be at least 32 characters")
	}
	sum := sha256.Sum256([]byte(key))
	block, err := aes.NewCipher(sum[:])
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Cookies{aead: aead, secure: secure}, nil
}

type sealed[T any] struct {
	Value   T     `json:"v"`
	Expires int64 `json:"e"`
}

func setCookie[T any](c *Cookies, w http.ResponseWriter, name string, v T, ttl time.Duration) error {
	plain, err := json.Marshal(sealed[T]{Value: v, Expires: time.Now().Add(ttl).Unix()})
	if err != nil {
		return err
	}
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return err
	}
	// The cookie name is bound in as additional data, so a value can't be
	// moved from one cookie to another.
	box := c.aead.Seal(nonce, nonce, plain, []byte(name))
	http.SetCookie(w, &http.Cookie{
		Name:     name,
		Value:    base64.RawURLEncoding.EncodeToString(box),
		Path:     "/",
		MaxAge:   int(ttl.Seconds()),
		HttpOnly: true,
		Secure:   c.secure,
		SameSite: http.SameSiteLaxMode,
	})
	return nil
}

func getCookie[T any](c *Cookies, r *http.Request, name string) (T, bool) {
	var zero T
	ck, err := r.Cookie(name)
	if err != nil {
		return zero, false
	}
	box, err := base64.RawURLEncoding.DecodeString(ck.Value)
	if err != nil || len(box) < c.aead.NonceSize() {
		return zero, false
	}
	n := c.aead.NonceSize()
	plain, err := c.aead.Open(nil, box[:n], box[n:], []byte(name))
	if err != nil {
		return zero, false
	}
	var s sealed[T]
	if json.Unmarshal(plain, &s) != nil || time.Now().Unix() > s.Expires {
		return zero, false
	}
	return s.Value, true
}

func clearCookie(c *Cookies, w http.ResponseWriter, name string) {
	http.SetCookie(w, &http.Cookie{Name: name, Path: "/", MaxAge: -1, HttpOnly: true, Secure: c.secure, SameSite: http.SameSiteLaxMode})
}
