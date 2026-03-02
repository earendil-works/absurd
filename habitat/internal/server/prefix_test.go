package server

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"habitat/internal/config"
)

func TestPublicBasePath(t *testing.T) {
	tests := []struct {
		name      string
		basePath  string
		headerVal string
		want      string
	}{
		{name: "base path only", basePath: "/habitat", want: "/habitat"},
		{name: "forwarded prefix only", headerVal: "/proxy", want: "/proxy"},
		{name: "forwarded prefix prepends base", basePath: "/habitat", headerVal: "/proxy", want: "/proxy/habitat"},
		{name: "forwarded already contains base", basePath: "/habitat", headerVal: "/proxy/habitat", want: "/proxy/habitat"},
		{name: "forwarded prefix collapses leading slashes", basePath: "/habitat", headerVal: "//proxy", want: "/proxy/habitat"},
		{name: "forwarded prefix uses last value", basePath: "/habitat", headerVal: "/client, /proxy", want: "/proxy/habitat"},
		{name: "forwarded prefix skips empty values", basePath: "/habitat", headerVal: "/client,   , /proxy", want: "/proxy/habitat"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := &Server{cfg: config.Config{BasePath: tc.basePath}}
			req := httptest.NewRequest("GET", "http://example.com/", nil)
			if tc.headerVal != "" {
				req.Header.Set("X-Forwarded-Prefix", tc.headerVal)
			}

			got := srv.publicBasePath(req)
			if got != tc.want {
				t.Fatalf("publicBasePath() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestWithBasePathRedirectUsesForwardedPrefix(t *testing.T) {
	srv := &Server{cfg: config.Config{BasePath: "/habitat"}}
	handler := srv.withBasePath(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected handler invocation for redirect request")
	}))

	req := httptest.NewRequest("GET", "http://example.com/habitat?x=1", nil)
	req.Header.Set("X-Forwarded-Prefix", "/proxy")

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusPermanentRedirect {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusPermanentRedirect)
	}

	location := resp.Header().Get("Location")
	if location == "" {
		t.Fatalf("expected redirect location")
	}

	parsed, err := url.Parse(location)
	if err != nil {
		t.Fatalf("parse redirect location %q: %v", location, err)
	}
	if parsed.Path != "/proxy/habitat/" {
		t.Fatalf("redirect path = %q, want %q", parsed.Path, "/proxy/habitat/")
	}
	if parsed.RawQuery != "x=1" {
		t.Fatalf("redirect query = %q, want %q", parsed.RawQuery, "x=1")
	}
}

func TestRenderIndexHTMLRewritesStaticPrefix(t *testing.T) {
	srv := &Server{
		indexHTML: []byte(`<html><head><script src="/_static/assets/app.js"></script></head><body></body></html>`),
	}

	output := string(srv.renderIndexHTML(uiRuntimeConfig{
		BasePath:       "/proxy/habitat",
		APIBasePath:    "/proxy/habitat/api",
		StaticBasePath: "/proxy/habitat/_static",
	}))

	if want := `src="/proxy/habitat/_static/assets/app.js"`; !strings.Contains(output, want) {
		t.Fatalf("expected output to contain %q, got %q", want, output)
	}
	if want := `window.__HABITAT_RUNTIME_CONFIG__`; !strings.Contains(output, want) {
		t.Fatalf("expected runtime config script in output")
	}
}
