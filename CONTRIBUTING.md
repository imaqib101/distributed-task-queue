# Contributing

Thanks for your interest in improving distributed-task-queue.

## Setup

Node.js 20 or newer is required (see `.nvmrc`).

```bash
npm ci
```

## Before opening a pull request

Run the same checks CI runs:

```bash
npm run lint
npm run format:check   # or `npm run format` to fix
npm run build
npm test
```

## Guidelines

- Add or update tests under `tests/` for every behaviour change or bug fix.
- Keep TypeScript strict; avoid `any` in new public APIs where practical.
- Use [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `docs:`, `test:`, `chore:`, `ci:`).
- Update `CHANGELOG.md` under "Unreleased" for user-visible changes.

## Reporting bugs

Open an issue with a minimal reproduction, expected versus actual behaviour, and your Node.js version.
