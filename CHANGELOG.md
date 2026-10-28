# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- ESLint and Prettier configuration, with `lint`, `format`, `format:check` and `typecheck` scripts.
- GitHub Actions CI running lint, format check, build and tests on Node 20 and 22.
- Direct tests for `DeadLetterQueue`, `categorizeError`, queue performance metrics and the public API.
- LICENSE, CONTRIBUTING, CHANGELOG, `.editorconfig`, `.nvmrc`; package metadata, `exports`, `files` and `engines`.

### Changed

- Stricter TypeScript: `noUncheckedIndexedAccess`, `noUnusedLocals`, `noUnusedParameters`, `noImplicitReturns`.

### Fixed

- `TaskQueue.getPerformanceMetrics` percentiles now use nearest-rank and match `WorkerPoolStatsTracker`.
- `DLQMonitor` and `WorkerPoolStatsTracker` are now exported from the package entry point.
- The `WorkerPoolStatsTracker` percentile test used a window smaller than its fixture data.

## [1.1.0]

- Worker pool concurrency, priority scheduling, adaptive concurrency, DLQ metrics and replay, pool statistics tracker.
