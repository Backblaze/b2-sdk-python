# Release Process

- Get the latest versions of dependencies
  - `pip install --upgrade --upgrade-strategy eager -r requirements.txt -r requirements-test.txt -r requirements-setup.txt -r requirements-doc.txt`
- Bump the version number to an even number.
  - version number is in: `b2/version.py` and `README.md`.
- Update the release history in README.md.
- Run full tests (currently: `pre-commit.sh`)
- Build docs locally (currently: `python setup.py develop; (cd doc; ./regenerate.sh) && echo ok`)
- Commit and push to GitHub, then wait for build to complete successfully.
- Tag in git and push tag to origin.  (Version tags look like "v0.4.6".)
- Upload to PyPI.
  - `cd ~/sandbox/b2-sdk-python`    # or wherever your git repository is
  - `rm -rf dist ; python setup.py sdist`
  - `twine upload dist/*`
- Install using pip and verify that it gets the correct version.
- Bump the version number to an odd number and commit.
- Push to GitHub again.
