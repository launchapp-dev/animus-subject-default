# Changelog

## v0.1.2 — 2026-05-29

### Fixed

- Advertise the `subject_kind:task` capability marker in both the runtime
  `initialize` handshake response and the static `plugin.toml` capability list.
  The host's plugin preflight at
  `crates/orchestrator-core/src/plugin_preflight/mod.rs` derives subject_kinds
  by scanning `capabilities` for entries prefixed `subject_kind:`. Without the
  marker, preflight reported "role `subject_kind:task` unsatisfied" even when
  the plugin was installed and covered the task surface. Mirrors the
  `$ui/web` marker pattern that `launchapp-dev/animus-web-ui` uses to
  advertise the web UI role.

### Changed

- Bumped `animus-protocol` git pin from `v0.1.8` to `v0.1.13` so the runtime,
  protocol, and subject-protocol crates track the same release that introduced
  the `*_main_with_capabilities` extension point.

## v0.1.1

- Release pipeline: cosign keyless signing + `.sha256` checksums on release
  artifacts.

## v0.1.0

- Initial release. Replaces the in-tree `InTreeTaskSubjectBackend`. Covers
  the full `TaskProvider` surface (`task/list`, `task/get`, `task/create`,
  `task/update`, `task/next`, `task/status`, checklist + dependency verbs,
  `task/watch`, `task/schema`, `health/check`).
