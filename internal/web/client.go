package web

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/jakehulberg/airlock/internal/api"
)

// Client calls the orchestrator on behalf of a signed-in user.
type Client struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
}

// APIError is a non-2xx response from the orchestrator.
type APIError struct {
	Status  int
	Message string
}

func (e *APIError) Error() string { return e.Message }

func (c *Client) request(ctx context.Context, u User, method, path string, body any) (*http.Response, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, rd)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set(api.HeaderUser, u.Subject)
	req.Header.Set(api.HeaderUserName, u.Name)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("orchestrator unreachable: %w", err)
	}
	if resp.StatusCode >= 300 {
		defer resp.Body.Close()
		var e api.Error
		_ = json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&e)
		if e.Error == "" {
			e.Error = resp.Status
		}
		return nil, &APIError{Status: resp.StatusCode, Message: e.Error}
	}
	return resp, nil
}

func (c *Client) do(ctx context.Context, u User, method, path string, body, out any) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	resp, err := c.request(ctx, u, method, path, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func esc(s string) string { return url.PathEscape(s) }

func (c *Client) Pools(ctx context.Context, u User) (pools []api.Pool, err error) {
	return pools, c.do(ctx, u, "GET", "/v1/pools", nil, &pools)
}

func (c *Client) Claims(ctx context.Context, u User) (claims []api.Claim, err error) {
	return claims, c.do(ctx, u, "GET", "/v1/claims", nil, &claims)
}

func (c *Client) Claim(ctx context.Context, u User, name string) (claim api.Claim, err error) {
	return claim, c.do(ctx, u, "GET", "/v1/claims/"+esc(name), nil, &claim)
}

func (c *Client) CreateClaim(ctx context.Context, u User, req api.CreateClaim) (claim api.Claim, err error) {
	return claim, c.do(ctx, u, "POST", "/v1/claims", req, &claim)
}

func (c *Client) ReleaseClaim(ctx context.Context, u User, name string) error {
	return c.do(ctx, u, "DELETE", "/v1/claims/"+esc(name), nil, nil)
}

func (c *Client) Tasks(ctx context.Context, u User, claim string) (tasks []api.Task, err error) {
	return tasks, c.do(ctx, u, "GET", "/v1/claims/"+esc(claim)+"/tasks", nil, &tasks)
}

func (c *Client) Task(ctx context.Context, u User, claim, id string) (task api.Task, err error) {
	return task, c.do(ctx, u, "GET", "/v1/claims/"+esc(claim)+"/tasks/"+esc(id), nil, &task)
}

func (c *Client) StartTask(ctx context.Context, u User, claim, prompt string) (task api.Task, err error) {
	return task, c.do(ctx, u, "POST", "/v1/claims/"+esc(claim)+"/tasks", api.CreateTask{Prompt: prompt}, &task)
}

// StreamTask returns the task's event stream. The caller closes it.
func (c *Client) StreamTask(ctx context.Context, u User, claim, id string) (io.ReadCloser, error) {
	resp, err := c.request(ctx, u, "GET", "/v1/claims/"+esc(claim)+"/tasks/"+esc(id)+"/stream", nil)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}
