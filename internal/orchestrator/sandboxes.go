package orchestrator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"github.com/jakehulberg/airlock/internal/api"
)

// agent-sandbox resources (kubernetes-sigs/agent-sandbox v1.0.x).
var (
	ClaimGVR    = schema.GroupVersionResource{Group: "extensions.agents.x-k8s.io", Version: "v1beta1", Resource: "sandboxclaims"}
	PoolGVR     = schema.GroupVersionResource{Group: "extensions.agents.x-k8s.io", Version: "v1beta1", Resource: "sandboxwarmpools"}
	SandboxGVR  = schema.GroupVersionResource{Group: "agents.x-k8s.io", Version: "v1beta1", Resource: "sandboxes"}
	claimPrefix = "airlock-"
)

// Labels and annotations Airlock puts on the claims it creates.
const (
	LabelManagedBy  = "app.kubernetes.io/managed-by"
	LabelOwner      = "airlock.jakehulberg.com/owner"       // hash of the OIDC subject (label values are limited)
	AnnotationOwner = "airlock.jakehulberg.com/owner-name"  // email or username, for display
	AnnotationDesc  = "airlock.jakehulberg.com/description" // optional, on a SandboxWarmPool
)

var (
	ErrNotFound   = errors.New("not found")
	ErrBadRequest = errors.New("bad request")
	ErrConflict   = errors.New("conflict")
	ErrNotReady   = errors.New("sandbox is not ready")
)

// Sandboxes creates and reads SandboxClaims in one namespace on behalf of users.
// A user only ever sees the claims labeled with their owner hash.
type Sandboxes struct {
	Dynamic      dynamic.Interface
	Kube         kubernetes.Interface
	Namespace    string
	PoolSelector string // label selector for the warm pools users may claim from
	DefaultTTL   time.Duration
	MaxTTL       time.Duration
	Now          func() time.Time
}

func ownerKey(user string) string {
	sum := sha256.Sum256([]byte(user))
	return hex.EncodeToString(sum[:16])
}

func (s *Sandboxes) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *Sandboxes) Pools(ctx context.Context) ([]api.Pool, error) {
	list, err := s.Dynamic.Resource(PoolGVR).Namespace(s.Namespace).List(ctx, metav1.ListOptions{LabelSelector: s.PoolSelector})
	if err != nil {
		return nil, err
	}
	pools := make([]api.Pool, 0, len(list.Items))
	for _, u := range list.Items {
		template, _, _ := unstructured.NestedString(u.Object, "spec", "sandboxTemplateRef", "name")
		replicas, _, _ := unstructured.NestedInt64(u.Object, "spec", "replicas")
		ready, _, _ := unstructured.NestedInt64(u.Object, "status", "readyReplicas")
		pools = append(pools, api.Pool{
			Name:        u.GetName(),
			Template:    template,
			Description: u.GetAnnotations()[AnnotationDesc],
			Replicas:    replicas,
			Ready:       ready,
		})
	}
	sort.Slice(pools, func(i, j int) bool { return pools[i].Name < pools[j].Name })
	return pools, nil
}

func (s *Sandboxes) pool(ctx context.Context, name string) error {
	sel, err := labels.Parse(s.PoolSelector)
	if err != nil {
		return err
	}
	u, err := s.Dynamic.Resource(PoolGVR).Namespace(s.Namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) || (err == nil && !sel.Matches(labels.Set(u.GetLabels()))) {
		return fmt.Errorf("%w: no pool named %q", ErrBadRequest, name)
	}
	return err
}

func (s *Sandboxes) List(ctx context.Context, user string) ([]api.Claim, error) {
	list, err := s.Dynamic.Resource(ClaimGVR).Namespace(s.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: LabelOwner + "=" + ownerKey(user),
	})
	if err != nil {
		return nil, err
	}
	claims := make([]api.Claim, 0, len(list.Items))
	for i := range list.Items {
		claims = append(claims, toClaim(&list.Items[i]))
	}
	sort.Slice(claims, func(i, j int) bool { return claims[i].Created.After(claims[j].Created) })
	return claims, nil
}

func (s *Sandboxes) Get(ctx context.Context, user, name string) (api.Claim, error) {
	u, err := s.get(ctx, user, name)
	if err != nil {
		return api.Claim{}, err
	}
	return toClaim(u), nil
}

func (s *Sandboxes) get(ctx context.Context, user, name string) (*unstructured.Unstructured, error) {
	u, err := s.Dynamic.Resource(ClaimGVR).Namespace(s.Namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	// Someone else's claim looks the same as no claim.
	if u.GetLabels()[LabelOwner] != ownerKey(user) {
		return nil, ErrNotFound
	}
	return u, nil
}

func (s *Sandboxes) Create(ctx context.Context, user, userName string, req api.CreateClaim) (api.Claim, error) {
	if req.Pool == "" {
		return api.Claim{}, fmt.Errorf("%w: pool is required", ErrBadRequest)
	}
	if req.Name != "" {
		if errs := validation.IsDNS1123Label(req.Name); len(errs) > 0 {
			return api.Claim{}, fmt.Errorf("%w: name %q: %s", ErrBadRequest, req.Name, errs[0])
		}
	}
	ttl := s.DefaultTTL
	if req.TTLMinutes > 0 {
		ttl = time.Duration(req.TTLMinutes) * time.Minute
	}
	if ttl <= 0 || ttl > s.MaxTTL {
		return api.Claim{}, fmt.Errorf("%w: ttl must be between 1m and %s", ErrBadRequest, s.MaxTTL)
	}
	if err := s.pool(ctx, req.Pool); err != nil {
		return api.Claim{}, err
	}

	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": ClaimGVR.GroupVersion().String(),
		"kind":       "SandboxClaim",
		"metadata": map[string]any{
			"namespace": s.Namespace,
			"labels": map[string]any{
				LabelManagedBy: "airlock",
				LabelOwner:     ownerKey(user),
			},
			"annotations": map[string]any{
				AnnotationOwner: userName,
			},
		},
		"spec": map[string]any{
			"warmPoolRef": map[string]any{"name": req.Pool},
			// Every claim expires. Delete takes the sandbox, and with it the
			// pod's Agent Vault session, down with the claim.
			"lifecycle": map[string]any{
				"shutdownPolicy": "Delete",
				"shutdownTime":   s.now().Add(ttl).UTC().Format(time.RFC3339),
			},
		},
	}}
	if req.Name != "" {
		u.SetName(req.Name)
	} else {
		u.SetGenerateName(claimPrefix)
	}

	created, err := s.Dynamic.Resource(ClaimGVR).Namespace(s.Namespace).Create(ctx, u, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return api.Claim{}, fmt.Errorf("%w: a claim named %q already exists", ErrConflict, req.Name)
	}
	if err != nil {
		return api.Claim{}, err
	}
	return toClaim(created), nil
}

func (s *Sandboxes) Delete(ctx context.Context, user, name string) error {
	u, err := s.get(ctx, user, name)
	if err != nil {
		return err
	}
	uid := u.GetUID()
	err = s.Dynamic.Resource(ClaimGVR).Namespace(s.Namespace).Delete(ctx, name, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &uid},
	})
	if apierrors.IsNotFound(err) {
		return ErrNotFound
	}
	return err
}

// Pod finds the running pod behind a user's ready claim.
func (s *Sandboxes) Pod(ctx context.Context, user, name string) (*corev1.Pod, error) {
	u, err := s.get(ctx, user, name)
	if err != nil {
		return nil, err
	}
	c := toClaim(u)
	if !c.Ready || c.Sandbox == "" || c.Deleting {
		return nil, ErrNotReady
	}
	sb, err := s.Dynamic.Resource(SandboxGVR).Namespace(s.Namespace).Get(ctx, c.Sandbox, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, ErrNotReady
	}
	if err != nil {
		return nil, err
	}
	// The Sandbox publishes a selector for its pod. A pod adopted from a warm
	// pool keeps its own name, so don't assume it matches the Sandbox's.
	selector, _, _ := unstructured.NestedString(sb.Object, "status", "selector")
	if selector == "" {
		pod, err := s.Kube.CoreV1().Pods(s.Namespace).Get(ctx, c.Sandbox, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil, ErrNotReady
		}
		return pod, err
	}
	pods, err := s.Kube.CoreV1().Pods(s.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, err
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Status.Phase == corev1.PodRunning && p.DeletionTimestamp == nil {
			return p, nil
		}
	}
	return nil, ErrNotReady
}

func toClaim(u *unstructured.Unstructured) api.Claim {
	c := api.Claim{
		Name:     u.GetName(),
		Owner:    u.GetAnnotations()[AnnotationOwner],
		Created:  u.GetCreationTimestamp().Time,
		Deleting: u.GetDeletionTimestamp() != nil,
	}
	c.Pool, _, _ = unstructured.NestedString(u.Object, "spec", "warmPoolRef", "name")
	c.Sandbox, _, _ = unstructured.NestedString(u.Object, "status", "sandbox", "name")
	if s, _, _ := unstructured.NestedString(u.Object, "spec", "lifecycle", "shutdownTime"); s != "" {
		c.ShutdownTime, _ = time.Parse(time.RFC3339, s)
	}
	conds, _, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
	for _, raw := range conds {
		cond, ok := raw.(map[string]any)
		if !ok || cond["type"] != "Ready" {
			continue
		}
		c.Ready = cond["status"] == "True"
		c.Reason, _ = cond["reason"].(string)
		c.Message, _ = cond["message"].(string)
	}
	return c
}
