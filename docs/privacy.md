# Privacy and Git Sharing

Agent Mesh state can contain participant identities, request and response text,
decision records, source paths, and uploaded attachments. Treat `.agent-mesh/` as project data,
not as harmless generated metadata.

## Privacy-first default

New projects use `local-only` state sharing unless `git-shared` is selected
explicitly:

```bash
agent-mesh init \
  --participants human,agent \
  --default-sender human \
  --default-recipient agent
```

The generated config records the choice:

```toml
[version_control]
state_sharing = "local-only"
```

In `local-only` mode, `.agent-mesh/.gitignore` contains a deny-all rule. The rule
also ignores the generated policy file itself, so a normal `git add -A` selects no
path under `.agent-mesh/`.

This protection applies to paths under `.agent-mesh/`. If a project configures
canonical paths or compatibility views elsewhere, those outputs follow the target
repository's own Git rules and require a separate privacy review. Do not describe
such a project as fully local-only merely because this setting is present.

## Explicit Git-shared mode

Use Git-shared state only when the people with access to the Git remote should be
able to read the canonical coordination history:

```bash
agent-mesh init \
  --participants human,agent \
  --default-sender human \
  --default-recipient agent \
  --state-sharing git-shared
```

Git-shared mode is still deny-by-default. It allows only these paths:

- `.agent-mesh/.gitignore`
- `.agent-mesh/config.toml`
- `.agent-mesh/events.jsonl`
- `.agent-mesh/bodies/**`

Databases, generated views, archives, Workbench bookmarks, `.agent-mesh/attachments`, lock
files, recovery journals, and future unknown files remain ignored. This is an
allowlist of canonical state, not a promise that the allowed files are public.
Review their contents before committing them.

## Existing projects and upgrades

Configs created before this setting existed are interpreted as `git-shared` for
backward compatibility. This avoids silently changing an established
collaboration workflow or suggesting that already tracked data became private.

To make an existing project local-only:

1. Add the following to `.agent-mesh/config.toml`:

   ```toml
   [version_control]
   state_sharing = "local-only"
   ```

2. Run `agent-mesh init` again to refresh the generated policy.
3. Remove any previously tracked Agent Mesh paths from the Git index while
   leaving local files in place:

   ```bash
   git rm -r --cached --ignore-unmatch -- .agent-mesh
   ```

4. Commit that removal and verify the current tree:

   ```bash
   git ls-files -- .agent-mesh
   ```

The final command should print nothing for a fully local-only project.

Changing `.gitignore` does not erase prior commits, forks, caches, or clones. If
sensitive Agent Mesh data was previously published, rotate any exposed secrets
and either rewrite every affected ref or publish from a reviewed clean history.

## Before making a repository public

Run these checks from the exact branch or clean publish checkout you will share:

```bash
git status --short
git ls-files -- .agent-mesh
git log --all --oneline -- .agent-mesh
git grep -n -I -E '(BEGIN [A-Z ]*PRIVATE KEY|api[_-]?key|access[_-]?token|password)'
```

Also inspect commit authors, remote URLs, documentation examples, screenshots,
recordings, archives, environment files, and package artifacts. Secret scanners
reduce risk but do not replace a human review of identities and project context.

The Agent Mesh package repository uses a separate positive publish manifest to
build its curated GitHub surface. `.gitignore` remains a conventional local
safety control; it is not used as the package release manifest.
