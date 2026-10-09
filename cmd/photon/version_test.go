package main

import "testing"

func TestIsRelease(t *testing.T) {
	for v, want := range map[string]bool{
		"v0.1.0":                               true,
		"v1.2.3":                               true,
		"v0.2.0-rc.1":                          true,
		"v0.1.1-0.20261009113759-a99b2ed2d2bb": false, // pseudo-version
		"v0.1.0+dirty":                         false, // modified checkout
		"(devel)":                              false,
		"(devel) a99b2ed2d2bb":                 false,
		"":                                     false,
	} {
		if got := isRelease(v); got != want {
			t.Errorf("isRelease(%q) = %v, want %v", v, got, want)
		}
	}
}
