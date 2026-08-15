# Releasing Agent Mesh

Only a human maintainer publishes Agent Mesh. Automation builds and uploads the
exact artifacts, but GitHub environment approval remains a direct human action.

## One-time publisher setup

1. Create accounts on PyPI and TestPyPI, verify the maintainer email address,
   enable two-factor authentication, and save recovery codes securely.
2. In both indexes, add a pending GitHub Trusted Publisher with these values:

   | Field | Value |
   |---|---|
   | PyPI project | `agent-mesh` |
   | GitHub owner | `cbalgeman` |
   | GitHub repository | `agent-mesh` |
   | Workflow filename | `publish.yml` |
   | PyPI environment | `pypi` |
   | TestPyPI environment | `testpypi` |

   Use the matching environment for each index. A pending publisher creates the
   project on first successful upload; it does not reserve the name beforehand.
3. In GitHub repository settings, create `testpypi` and `pypi` environments.
   Require a maintainer review for `pypi`. Restrict deployments to release tags
   that match `v*`. A reviewer gate for `testpypi` is optional.
4. Do not add PyPI usernames, passwords, or API tokens as repository secrets.
   The workflow exchanges GitHub's short-lived OpenID Connect identity for an
   index-scoped publishing token.

## Prepare a release candidate

1. Start from the curated public branch, not the broader development branch.
2. Set the same version in `pyproject.toml`,
   `src/agent_mesh/__init__.py`, and the heading in `CHANGELOG.md`.
3. Replace `Unreleased` in the changelog heading with the release date and use
   that section as the GitHub release notes.
4. Build and inspect the exact distributions locally:

   ```bash
   python -m pip install --upgrade build twine
   python -m build
   python -m twine check dist/*
   python -m zipfile --list dist/*.whl
   python -m tarfile --list dist/*.tar.gz
   ```

5. Run the public CI contract:

   ```bash
   python -m pip install -e ".[test]"
   python -m ruff check src tests/public
   python -m pytest -q
   ```

6. Confirm that the repository contains only the intended public files and no
   credentials, private project state, local paths, or internal-only docs.

## TestPyPI rehearsal

After the human maintainer pushes the curated branch and public CI passes, run
the `Publish Python distribution` workflow manually. The manual trigger builds
once, checks the metadata, and publishes those artifacts only to TestPyPI.

Verify from a clean environment without consulting PyPI for dependencies:

```bash
python -m venv /tmp/agent-mesh-testpypi
/tmp/agent-mesh-testpypi/bin/python -m pip install \
  --index-url https://test.pypi.org/simple/ --no-deps agent-mesh==0.3.0
/tmp/agent-mesh-testpypi/bin/agent-mesh --help
/tmp/agent-mesh-testpypi/bin/agent-q --help
/tmp/agent-mesh-testpypi/bin/python -c \
  'import agent_mesh; print(agent_mesh.__version__)'
```

If the first upload reports that the project name is unavailable, stop. Choose
a different distribution name deliberately; the CLI commands and Python import
package do not have to change with it.

## Production publication

1. Confirm public CI passed for the exact curated commit and the TestPyPI
   rehearsal installed and ran successfully.
2. Create the tag `v0.3.0` at that exact commit and publish a normal GitHub
   release. Copy the `0.3.0` changelog section into the release notes.
3. The release event builds a fresh artifact set, verifies that the tag matches
   the package version, and pauses at the protected `pypi` environment.
4. The human maintainer reviews and approves the production deployment. The
   workflow then uploads through Trusted Publishing and emits index-hosted
   attestations.
5. Verify the public package from a clean environment:

   ```bash
   python -m venv /tmp/agent-mesh-pypi
   /tmp/agent-mesh-pypi/bin/python -m pip install agent-mesh==0.3.0
   /tmp/agent-mesh-pypi/bin/python -c \
     'import agent_mesh; print(agent_mesh.__version__)'
   /tmp/agent-mesh-pypi/bin/agent-mesh --help
   /tmp/agent-mesh-pypi/bin/agent-q --help
   ```

6. Confirm the PyPI project page shows the README, project links, source and
   wheel files, and expected Python requirement.

Published filenames and versions are immutable. Fix a bad release with a new
version; do not attempt to replace an existing artifact.
