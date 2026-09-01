package dao

import "testing"

func TestEscapeLike(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"plain", "abc 123", "abc 123"},
		{"percent", "50%", `50\%`},
		{"underscore", "a_b", `a\_b`},
		{"backslash", `a\b`, `a\\b`},
		{"all specials", `%_\`, `\%\_\\`},
		{"mixed keyword", `100%_off\sale`, `100\%\_off\\sale`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := escapeLike(tt.in); got != tt.want {
				t.Errorf("escapeLike(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
