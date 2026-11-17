# Release-note sources

Add a fragment with `go run github.com/miniscruff/changie@v1.24.0 new`.
Unreleased YAML fragments belong in `unreleased/`.

`v2.13.5.md` preserves the complete changelog history through v2.13.5, imported
without changing any released section. Future batches create one file per version.
Keep this historical archive intact. Regenerate `CHANGELOG.md` with
`go run github.com/miniscruff/changie@v1.24.0 merge` after editing a batch or header.
