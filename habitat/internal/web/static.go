package web

import (
	"embed"
	"io/fs"
)

// content embeds the production UI bundle produced by Vite.
//
//go:embed all:dist
var content embed.FS

// StaticFS returns the embedded static assets. The returned filesystem is rooted
// at the contents of the dist directory.
func StaticFS() (fs.FS, error) {
	return fs.Sub(content, "dist")
}

// IndexHTML reads the embedded index.html if present.
func IndexHTML() ([]byte, error) {
	return content.ReadFile("dist/index.html")
}
