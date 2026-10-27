package web

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

const (
	sessionCookie = "airlock_session"
	loginCookie   = "airlock_login"
)

// User is the signed-in person, from their OIDC ID token.
type User struct {
	Subject string `json:"sub"`
	Name    string `json:"name"` // email, else username, else subject
}

// Auth signs users in with OIDC (authorization code flow with PKCE).
type Auth struct {
	oauth         oauth2.Config
	verifier      *oidc.IDTokenVerifier
	cookies       *Cookies
	allowedGroups []string
	sessionTTL    time.Duration
}

type AuthConfig struct {
	IssuerURL     string
	ClientID      string
	ClientSecret  string
	RedirectURL   string   // https://<public host>/callback
	AllowedGroups []string // if set, the user's "groups" claim must contain one
	SessionTTL    time.Duration
}

func NewAuth(ctx context.Context, cfg AuthConfig, c *Cookies) (*Auth, error) {
	provider, err := oidc.NewProvider(ctx, cfg.IssuerURL)
	if err != nil {
		return nil, fmt.Errorf("oidc discovery: %w", err)
	}
	scopes := []string{oidc.ScopeOpenID, "profile", "email"}
	if len(cfg.AllowedGroups) > 0 {
		scopes = append(scopes, "groups")
	}
	return &Auth{
		oauth: oauth2.Config{
			ClientID:     cfg.ClientID,
			ClientSecret: cfg.ClientSecret,
			Endpoint:     provider.Endpoint(),
			RedirectURL:  cfg.RedirectURL,
			Scopes:       scopes,
		},
		verifier:      provider.Verifier(&oidc.Config{ClientID: cfg.ClientID}),
		cookies:       c,
		allowedGroups: cfg.AllowedGroups,
		sessionTTL:    cfg.SessionTTL,
	}, nil
}

type pendingLogin struct {
	State    string `json:"s"`
	Nonce    string `json:"n"`
	Verifier string `json:"v"`
	Next     string `json:"x"`
}

func randomString() string {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

// safeNext only allows local paths, so /login can't be used as an open redirect.
func safeNext(next string) string {
	if !strings.HasPrefix(next, "/") || strings.HasPrefix(next, "//") || strings.HasPrefix(next, "/\\") {
		return "/"
	}
	return next
}

func (a *Auth) Login(w http.ResponseWriter, r *http.Request) {
	p := pendingLogin{
		State:    randomString(),
		Nonce:    randomString(),
		Verifier: oauth2.GenerateVerifier(),
		Next:     safeNext(r.URL.Query().Get("next")),
	}
	if err := setCookie(a.cookies, w, loginCookie, p, 10*time.Minute); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, a.oauth.AuthCodeURL(p.State, oidc.Nonce(p.Nonce), oauth2.S256ChallengeOption(p.Verifier)), http.StatusFound)
}

func (a *Auth) Callback(w http.ResponseWriter, r *http.Request) {
	p, ok := getCookie[pendingLogin](a.cookies, r, loginCookie)
	clearCookie(a.cookies, w, loginCookie)
	if !ok || r.URL.Query().Get("state") != p.State {
		http.Error(w, "login expired, try again", http.StatusBadRequest)
		return
	}
	if e := r.URL.Query().Get("error"); e != "" {
		http.Error(w, "sign-in failed: "+e, http.StatusForbidden)
		return
	}
	user, err := a.exchange(r.Context(), r.URL.Query().Get("code"), p)
	if err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	if err := setCookie(a.cookies, w, sessionCookie, user, a.sessionTTL); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, p.Next, http.StatusFound)
}

func (a *Auth) exchange(ctx context.Context, code string, p pendingLogin) (User, error) {
	tok, err := a.oauth.Exchange(ctx, code, oauth2.VerifierOption(p.Verifier))
	if err != nil {
		return User{}, errors.New("sign-in failed: could not exchange code")
	}
	raw, ok := tok.Extra("id_token").(string)
	if !ok {
		return User{}, errors.New("sign-in failed: no ID token")
	}
	idToken, err := a.verifier.Verify(ctx, raw)
	if err != nil || idToken.Nonce != p.Nonce {
		return User{}, errors.New("sign-in failed: invalid ID token")
	}
	var claims struct {
		Email             string   `json:"email"`
		PreferredUsername string   `json:"preferred_username"`
		Groups            []string `json:"groups"`
	}
	if err := idToken.Claims(&claims); err != nil {
		return User{}, errors.New("sign-in failed: unreadable ID token")
	}
	if len(a.allowedGroups) > 0 && !slices.ContainsFunc(claims.Groups, func(g string) bool {
		return slices.Contains(a.allowedGroups, g)
	}) {
		return User{}, errors.New("you are not in a group that may use Airlock")
	}
	name := cmp.Or(claims.Email, claims.PreferredUsername, idToken.Subject)
	// Scope the subject to its issuer, so two providers can't share an owner.
	return User{Subject: idToken.Issuer + "#" + idToken.Subject, Name: name}, nil
}

func (a *Auth) Logout(w http.ResponseWriter, r *http.Request) {
	clearCookie(a.cookies, w, sessionCookie)
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

type userCtxKey struct{}

// Require serves only signed-in users. Page loads are sent to sign in; other
// requests get 401.
func (a *Auth) Require(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, ok := getCookie[User](a.cookies, r, sessionCookie)
		if !ok {
			if r.Method == http.MethodGet && !strings.HasSuffix(r.URL.Path, "/stream") {
				http.Redirect(w, r, "/login?next="+url.QueryEscape(r.URL.RequestURI()), http.StatusFound)
				return
			}
			http.Error(w, "sign in first", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), userCtxKey{}, user)))
	})
}

func userFrom(ctx context.Context) User {
	u, _ := ctx.Value(userCtxKey{}).(User)
	return u
}
