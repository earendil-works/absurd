package config

import "testing"

func TestNormalizeBasePath(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "empty", input: "", want: ""},
		{name: "root", input: "/", want: ""},
		{name: "adds leading slash", input: "habitat", want: "/habitat"},
		{name: "trims trailing slash", input: "/habitat/", want: "/habitat"},
		{name: "rejects query", input: "/habitat?q=1", wantErr: true},
		{name: "rejects network-path style", input: "//habitat", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeBasePath(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q", tc.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for %q: %v", tc.input, err)
			}
			if got != tc.want {
				t.Fatalf("normalizeBasePath(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}
