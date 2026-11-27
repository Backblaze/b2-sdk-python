# Testing

Run commands from the repository root. Install the tools described in [CONTRIBUTING.md](../CONTRIBUTING.md).
The examples use Python 3.13. Use `nox -l` to list the sessions for your installed versions.

## Test levels

| Where it lives | Command | What it proves | What it does not prove |
| --- | --- | --- | --- |
| [Unit](../test/unit) | `nox -s unit-3.13` | Runs the unit suite for each API version, v0 through v3. The simulator-backed tests talk to b2-simulator over HTTP. A few marked tests use the in-process [`RawSimulator`](../b2sdk/_internal/raw_simulator.py). | It does not prove live B2 behavior. Not every unit test uses a simulator. |
| [Static](../test/static) | `nox -s lint` | Runs checks for Python license headers, workflow gates, JUnit reporting, documentation links, and other repository regressions. The session also runs Ruff and license checks. | It does not run unit or live B2 tests. |
| [Integration](../test/integration) | `nox -s integration-3.13` | Exercises SDK operations against live B2 when credentials are configured. | It does not cover every service behavior or every SDK path. |
| [sdkharness contract](../.sdkharness/README.md) | `SDKHARNESS_TEST_LEVEL=conformance SDKHARNESS_SCENARIO=files.upload .sdkharness/tests/run-conformance` | Exercises the selected contract against a loopback simulator. Follow the harness README to start the simulator and set its URL first. | It does not test live B2. |
| [Known defects](../test/known_defect.py) | `uv run --group test pytest --runxfail -m known_defect --api=v3 test/unit` | Runs marked defect checks as ordinary tests so their failures are visible. | It does not prove that unmarked behavior is correct. |

The unit session selects each API version with the `--api` option.

## Unit tests on b2-simulator

The unit tests run against b2-simulator (`backblaze-labs/b2-simulator`) over HTTP by default.
They need a checkout of it. CI checks out the release pinned as `SIMULATOR_REVISION` in
[`ci.yml`](../.github/workflows/ci.yml) and [`cd.yml`](../.github/workflows/cd.yml).
The b2-simulator repository is not public yet, so only people with access to it can run the unit tests this way,
and CI refuses pull requests from forks.

1. Clone b2-simulator at the pinned revision. It needs Node.js 20.6 or later.
2. Set `B2_SIMULATOR_DIR` to the clone.
3. Run `nox -s unit-3.13`, or `uv run --group test pytest --api=v3 -n auto test/unit`.

If `B2_SIMULATOR_DIR` is missing or wrong, or `node` is not on `PATH`, the run stops with a usage error. It does not skip tests.
`--rawsimulator` runs the whole unit suite on the in-process `RawSimulator` instead and needs no checkout.
It deselects the tests marked `needs_b2_simulator`: they assert answers of real B2 that `RawSimulator` does not give.
Integration and static runs never need a checkout.

The plugin in [`test/unit/b2_simulator.py`](../test/unit/b2_simulator.py) starts one simulator for each pytest process.
With pytest-xdist, each worker gets its own simulator. The plugin resets the simulator before each test.
The reset sets the part sizes to `RawSimulator.MIN_PART_SIZE` (200 bytes).
Use `@pytest.mark.b2_simulator_config(...)` to send other reset settings for one test.
The raw API's `MIN_PART_SIZE` attribute stays 200 bytes; a test that changes the part size must not size its data from it.
A simulator stops when the pytest process that started it stops.

Shared setups get their raw API class from `raw_api_class()`: the `b2api` fixture, `TestCaseWithBucket`, and the other
per-class setups. Tests that need a server condition, such as an injected upload error, an expired token, a cut-short
download or a suspended notification rule, use the hooks in [`test/unit/simulator_hooks.py`](../test/unit/simulator_hooks.py).
`@pytest.mark.b2_simulator_config(extraAccounts=1)` gives a test a second account: the raw API's second `create_account()` call returns it.

### Tests that stay on RawSimulator

A test stays on the in-process `RawSimulator` only with `@pytest.mark.rawsimulator(reason=...)`.
The reason says why: the test checks `RawSimulator` itself, or it names the issue that blocks the move.
`from_ver` and `to_ver` limit the marker to some API versions, as for `apiver`.
An unmarked unit test that builds a `RawSimulator`, or a subclass of it, fails and names the marker.
Remove the marker when its issue is fixed, and run the test on b2-simulator.

The integration fixture requires `B2_TEST_APPLICATION_KEY_ID` and `B2_TEST_APPLICATION_KEY`.
If either variable is unset, the credential fixture fails instead of skipping the test.
Run integration tests only with an appropriate test account.
The harness accepts a literal IPv4 loopback simulator URL and uses fixed test credentials.
It does not use real B2 credentials. See the harness README for setup and cleanup.

The static test suite does not validate changelog fragments. Run `nox -s towncrier_check` for that check.

## Known-defect tests

Use `known_defect` to mark a test that asserts correct behavior for a defect that remains open.
Set `raises` to the expected failure type. Keep setup assertions separate from the defect assertion.
Use `expect` for the defect assertion when setup assertions must fail normally.
`expect` raises `DefectStillPresent` when the condition is false.
Use `defect_raises` to convert a specific exception and optional message into `DefectStillPresent`.
An unexpected exception remains a failure.

The marker uses strict expected failure. If the test unexpectedly passes, pytest reports `XPASS(strict)` as a failure.
Remove the marker when the fix makes the test pass, and keep the test as a regression test.
See [issue #689](https://github.com/Backblaze/b2-sdk-python/issues/689) for an example involving stale demonstrators.

## Showing a test pins the fix

1. Write a test that asserts the expected behavior.
2. Run the test and observe the failure.
3. Implement the fix and run the test again.
4. Temporarily revert the fix and confirm that the test fails again.
5. Restore the fix and confirm that the test passes.

## When a PR needs an integration test

Use an integration test when a change depends on live B2 behavior that the simulator cannot establish.
Examples include a service response, authorization outcome, or server-side transfer behavior.
A simulator test can check how the SDK handles a modeled response; it cannot prove that B2 sends that response.

## Reporting what you ran

Report the commands and their outcomes. Report a check that did not run as **NOT RUN**, never as passed.
State whether integration or harness checks used live B2 or a simulator.
