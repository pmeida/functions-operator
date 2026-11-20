/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package manifests

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	v1 "github.com/google/go-containerregistry/pkg/v1"
)

// extractLayers flattens the image's /manifests directory into
// dir/<manifests>, iterating layers in order with later layers winning, the
// same shape OLM v1's unpacker uses. Only regular files and directories are
// materialized; whiteouts and symlinks are ignored, which is all manifest
// data ever needs.
func extractLayers(img v1.Image, dest string) error {
	layers, err := img.Layers()
	if err != nil {
		return err
	}

	for _, layer := range layers {
		rc, err := layer.Uncompressed()
		if err != nil {
			return err
		}
		err = extractLayer(rc, dest)
		if closeErr := rc.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func extractLayer(r io.Reader, dest string) error {
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		name := filepath.Clean(hdr.Name)
		// Layer content is untrusted data: a tar entry must never escape
		// the destination directory.
		target := filepath.Join(dest, name)
		if target != dest && !strings.HasPrefix(target, dest+string(os.PathSeparator)) {
			continue
		}
		if strings.HasPrefix(filepath.Base(name), ".wh.") {
			// Whiteout: PoC ignores deletions.
			continue
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o750); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := writeFile(tr, target, hdr.Name); err != nil {
				return err
			}
		}
	}
}

func writeFile(r io.Reader, target, name string) error {
	if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
		return err
	}
	f, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o640)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, r); err != nil {
		_ = f.Close()
		return fmt.Errorf("copying %s: %w", name, err)
	}
	return f.Close()
}
