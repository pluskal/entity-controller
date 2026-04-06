# Agent Instructions — entity-controller

These instructions apply to every automated agent working on this repository.
Follow them precisely for every code-change session.

---

## 1. Commit conventions

Use **Conventional Commits** (`https://www.conventionalcommits.org`).

| Prefix | When to use |
|--------|-------------|
| `feat:` | New user-visible feature or config key |
| `fix:` | Bug fix |
| `docs:` | Documentation-only change (README, CHANGELOG, comments) |
| `chore:` | Build, tooling, version-bump, CI |
| `test:` | Adding or fixing tests |
| `refactor:` | Code restructure with no behaviour change |

Examples:

```
feat: add grace_period config option for cloud integrations
fix: block_timer_expires leaves controller stuck in blocked state
chore: bump version to 9.8.1
docs: add grace_period section to README
test: add behavioral tests for forced_sensors
```

Scope may be added in parentheses when useful, e.g. `fix(state-machine): …`.

---

## 2. Version bumping

The integration version lives in **one place only**:

```
custom_components/entity_controller/manifest.json  →  "version": "X.Y.Z"
```

### Semver rules

| Change type | Bump |
|-------------|------|
| Breaking change / major new feature set | **major** (X) |
| New backwards-compatible feature or config key | **minor** (Y) |
| Bug fix, documentation, chore | **patch** (Z) |

### Steps

1. Edit `manifest.json` – update `"version"`.
2. Add a matching section to `CHANGELOG.md` (see §3).
3. Commit with `chore: bump version to X.Y.Z`.

Do **not** update `package.json` or `postbump.js` — they are legacy artefacts and not used by HA.

---

## 3. Changelog format

File: **`CHANGELOG.md`** at the repository root.

### Structure

New entries go at the **top** of the file, immediately after the header block:

```markdown
<a name="X.Y.Z"></a>
## [X.Y.Z](https://github.com/pluskal/entity-controller/compare/vA.B.C...vX.Y.Z) (YYYY-MM-DD)


### Features

* **feature-name** – Description of what was added and why.

### Bug Fixes

* **`config_key`** – Description of the bug and the fix.

### Tests

* Short description of new/changed tests.
```

### Rules

* Only include sections that have content (`### Features`, `### Bug Fixes`, `### Tests`).
* Each entry for a new feature or config key belongs in the **same release** as the feature itself. Do **not** mix it into an older release's section.
* Bug-fix releases (patch bumps) typically contain only `### Bug Fixes`.
* Use present-tense, imperative bullets (e.g. "Add …", "Fix …").
* Link the version header to the GitHub comparison URL as shown above.
* Do **not** delete or alter existing entries — only add new ones.

---

## 4. Documentation (README.md)

File: **`README.md`** at the repository root.

### When to update

* Any new configuration key → add an entry to the **Configuration** table and a dedicated subsection explaining the option, default value, YAML example, and typical use-case.
* Any removed or renamed configuration key → mark it as deprecated/removed in the relevant section.
* Bug fixes that change observable behaviour → update any affected example snippets.

### Style

* Match the existing heading level and formatting style.
* YAML examples must be valid and minimal (no unnecessary options).
* Keep the **Configuration** reference table in sync with the prose sections.

---

## 5. Tests

* Tests live in `tests/`.
* New features must have corresponding tests in `tests/test_new_features.py` or a suitably named new file.
* Bug fixes should have a regression test.
* Run tests with:

  ```bash
  python -m pytest tests/test_legacy_behaviors.py tests/test_new_features.py --override-ini="addopts=" -q
  ```

* Do **not** remove or skip existing tests. If a test is wrong, fix it.

---

## 6. End-of-session checklist

Before finishing a session, verify:

- [ ] `manifest.json` version is correct.
- [ ] `CHANGELOG.md` has a new section matching the version.
- [ ] `README.md` is updated if any config keys or behaviours changed.
- [ ] Tests pass (see §5).
- [ ] All commits follow §1.
- [ ] `report_progress` has been called to push changes.
