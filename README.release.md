# Release Process

- Get the latest versions of dependencies
  - `pip install --upgrade --upgrade-strategy eager -r requirements.txt -r requirements-test.txt -r requirements-setup.txt -r requirements-doc.txt`
- Bump the version number to an even number in `b2/version.py`.
- Update the release history in README.md by changing "not released yet" to the current date for this release.
- Run full tests (currently: `pre-commit.sh`)
- Build docs locally (currently: `python setup.py develop; (cd doc; ./regenerate.sh) && echo ok`)
- Commit and push to GitHub, then wait for build to complete successfully.
- Tag in git and push tag to origin.  (Version tags look like "v0.4.6".)
- Upload to PyPI.
  - `cd ~/sandbox/b2-sdk-python`    # or wherever your git repository is
  - `rm -rf dist ; python setup.py sdist`
  - `twine upload dist/*`
- Install using pip and verify that it gets the correct version.
- Update for dev
  - Bump the version number
  - Add a "not released yet" section in the release history, like: 0.8.4 (not released yet)
  - check in
- Push to GitHub again.