# B2 Python SDK
&nbsp;[![Continuous Integration](https://github.com/Backblaze/b2-sdk-python/workflows/Continuous%20Integration/badge.svg)](https://github.com/Backblaze/b2-sdk-python/actions?query=workflow%3A%22Continuous+Integration%22)&nbsp;[![License](https://img.shields.io/pypi/l/b2sdk.svg?label=License)](https://pypi.python.org/pypi/b2)&nbsp;[![python versions](https://img.shields.io/pypi/pyversions/b2sdk.svg?label=python%20versions)](https://pypi.python.org/pypi/b2sdk)&nbsp;[![PyPI version](https://img.shields.io/pypi/v/b2sdk.svg?label=PyPI%20version)](https://pypi.python.org/pypi/b2sdk)&nbsp;[![Docs](https://readthedocs.org/projects/b2-sdk-python/badge/?version=master)](https://b2-sdk-python.readthedocs.io/en/master/)



This repository contains a client library and a few handy utilities for easy access to all of the capabilities of B2 Cloud Storage.

[B2 command-line tool](https://github.com/Backblaze/B2_Command_Line_Tool) is an example of how it can be used to provide command-line access to the B2 service, but there are many possible applications (including FUSE filesystems, storage backend drivers for backup applications etc).

# Documentation

The latest documentation is available on [Read the Docs](https://b2-sdk-python.readthedocs.io).

# Installation

The sdk can be installed with:

    pip install b2sdk

# Version policy

b2sdk follows [Semantic Versioning](https://semver.org/) policy, so in essence the version number is MAJOR.MINOR.PATCH (for example 1.2.3) and:
- we increase MAJOR version when we make incompatible API changes
- we increase MINOR version when we add functionality in a backwards-compatible manner, and
- we increase PATCH version when we make backwards-compatible bug fixes (unless someone relies on the undocumented behavior of a fixed bug)

Therefore when setting up b2sdk as a dependency, please make sure to match the version appropriately, for example you could put this in your `requirements.txt` to make sure your code is compatible with the `b2sdk` version your user will get from pypi:

```
b2sdk>=2,<3
```

# Release History

Please refer to the [changelog](CHANGELOG.md).

# Developer Info

Please see our [contributing guidelines](CONTRIBUTING.md).
