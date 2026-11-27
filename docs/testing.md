# Testing

Run commands from the repository root. Install the tools described in [CONTRIBUTING.md](../CONTRIBUTING.md).
The examples use Python 3.13. Use `nox -l` to list the sessions for your installed versions.

## Test levels

| Where it lives | Command | What it proves | What it does not prove |
| --- | --- | --- | --- |
| [Unit](../test/unit) | `nox -s unit-3.13` | Runs the unit suite for each API version, v0 through v3. Tests that use [`RawSimulator`](../b2sdk/_internal/raw_simulator.py) exercise SDK behavior against an in-memory simulator. | It does not prove live B2 behavior. Not every unit test uses the simulator. |
| [Static](../test/static) | `nox -s lint` | Runs checks for Python license headers, workflow gates, JUnit reporting, documentation links, and other repository regressions. The session also runs Ruff and license checks. | It does not run unit or live B2 tests. |
| [Integration](../test/integration) | `nox -s integration-3.13` | Exercises SDK operations against live B2 when credentials are configured. CI runs it against the B2 Simulator for pull requests, and against live B2 weekly and before each release. | It does not cover every service behavior or every SDK path. |
| [sdkharness contract](../.sdkharness/README.md) | `SDKHARNESS_TEST_LEVEL=conformance SDKHARNESS_SCENARIO=files.upload .sdkharness/tests/run-conformance` | Exercises the selected contract against a loopback simulator. Follow the harness README to start the simulator and set its URL first. | It does not test live B2. |
| [Known defects](../test/known_defect.py) | `uv run --group test pytest --runxfail -m known_defect --api=v3 test/unit` | Runs marked defect checks as ordinary tests so their failures are visible. | It does not prove that unmarked behavior is correct. |

The unit session selects each API version with the `--api` option.

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
