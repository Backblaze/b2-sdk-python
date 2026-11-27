# B2 Python SDK
&nbsp;[![Continuous Integration](https://github.com/Backblaze/b2-sdk-python/workflows/Continuous%20Integration/badge.svg)](https://github.com/Backblaze/b2-sdk-python/actions?query=workflow%3A%22Continuous+Integration%22)&nbsp;[![License](https://img.shields.io/pypi/l/b2sdk.svg?label=License)](https://pypi.python.org/pypi/b2)&nbsp;[![python versions](https://img.shields.io/pypi/pyversions/b2sdk.svg?label=python%20versions)](https://pypi.python.org/pypi/b2sdk)&nbsp;[![PyPI version](https://img.shields.io/pypi/v/b2sdk.svg?label=PyPI%20version)](https://pypi.python.org/pypi/b2sdk)&nbsp;[![Docs](https://readthedocs.org/projects/b2-sdk-python/badge/?version=master)](https://b2-sdk-python.readthedocs.io/en/master/)



This repository contains a client library and a few handy utilities. They provide access to all capabilities of B2 Cloud Storage.

[B2 command-line tool](https://github.com/Backblaze/B2_Command_Line_Tool) uses the library to provide command-line access to the B2 service. Other applications can include FUSE filesystems and storage backend drivers for backup applications.

# Documentation

The latest documentation is available on [Read the Docs](https://b2-sdk-python.readthedocs.io).

# Installation

You can install the SDK with:

    pip install b2sdk

To install with all optional dependencies:

    pip install 'b2sdk[full]'

# Version policy

b2sdk follows [Semantic Versioning](https://semver.org/) policy, so in essence the version number is MAJOR.MINOR.PATCH (for example 1.2.3) and:
- we increase MAJOR version when we make incompatible API changes
- we increase MINOR version when we add functionality in a backwards-compatible manner, and
- we increase PATCH version when we make backwards-compatible bug fixes (unless someone relies on the undocumented behavior of a fixed bug)

When you add b2sdk as a dependency, choose a version range that matches your code. For example, you can put this in your `requirements.txt` to select a compatible `b2sdk` version from pypi:

```
b2sdk>=2,<3
```

# Release History

Please refer to the [changelog](CHANGELOG.md).

# Related projects

- **[b2-mcp](https://github.com/backblaze-labs/b2-mcp)** — a Model Context Protocol server for B2.

# Developer Info

Please see our [contributing guidelines](CONTRIBUTING.md).
The [testing guide](docs/testing.md) and the
[writing style guide](docs/writing-style.md) describe how to test changes and write documentation.
[llms.txt](llms.txt) lists the user documentation.
