# sdkharness contract

This directory exposes version-matched quality checks to the centralized
[`sdkharness`](https://github.com/backblaze-labs/demand-side-ai/tree/main/sdkharness).
The repository owns the executable assertions; sdkharness owns the canonical
scenario, simulator, invocation, evidence, fleet report, and notification.

`tests.tsv` is the machine-readable entry point. Schema 1 has four
tab-separated columns: `test_level`, `scenario`, `target`, and `executable`.
Executables print one five-field, tab-separated result:

```text
SDKHARNESS_RESULT	health	golden-path	PASS	-
```

The customer-health, conformance, and resilience executables accept only
literal IPv4 loopback HTTP simulator URLs. They import `b2sdk` from this exact
checkout and never use a production B2 endpoint or real credentials. The
individual conformance and resilience check files refuse to run at all unless
they are given a loopback simulator URL (they use only the fixed simulator
credential `test-key-id` / `test-key`), so run them through the dispatchers
below, never with real `B2_*` values in the environment.

Conformance owns 33 capability checks and resilience owns 16 injected-fault
checks. Their small dispatchers validate the invocation, run the selected
repository-owned assertion, and translate its standing verdict into the
five-field `SDKHARNESS_RESULT` record. The central harness continues to own
scenario selection, simulator lifecycle, fleet evidence, issue reconciliation,
reporting, and notification.

## Run one check locally

Nothing here touches B2. You need Python 3.10+ and Node 22+ (for the simulator).

```bash
# 1. This checkout, in a virtualenv (the checks import b2sdk from here)
python -m venv .venv && . .venv/bin/activate && pip install -e .

# 2. A local simulator (any one of these; it needs access to backblaze-labs/b2-simulator)
git clone https://github.com/backblaze-labs/b2-simulator /tmp/b2-simulator
node /tmp/b2-simulator/bin/simulator/serve.mjs --control > /tmp/sim.out &    # prints the URLs
# ...or use the simulator embedded in the harness: sdkharness/bin/simulator/serve.mjs

# 3. Read the URLs it printed
export SDKHARNESS_SIMULATOR_URL=$(sed -n 's/^SIMULATOR-LISTENING \(http:.*\)/\1/p' /tmp/sim.out)
export SDKHARNESS_SIMULATOR_HTTPS_URL=$(sed -n 's/^SIMULATOR-LISTENING \(https:.*\)/\1/p' /tmp/sim.out)
export SDKHARNESS_SIMULATOR_CONTROL_URL=$(sed -n 's/^SIMULATOR-CONTROL \(.*\)/\1/p' /tmp/sim.out)
export SDKHARNESS_SIMULATOR_CA=/tmp/b2-simulator/bin/simulator/loopback-cert.pem
```

The standalone simulator also exports the same values as `B2SIM_URL`,
`B2SIM_HTTPS_URL`, `B2SIM_CONTROL_URL` and `B2SIM_CA` (its `bin/lib/simulator.sh`
helper); the checks read the `SDKHARNESS_SIMULATOR_*` names above.

```bash
# conformance (one capability)
SDKHARNESS_TEST_LEVEL=conformance SDKHARNESS_SCENARIO=files.upload .sdkharness/tests/run-conformance

# resilience (one injected fault; needs the control URL, i.e. serve.mjs --control)
SDKHARNESS_TEST_LEVEL=resilience SDKHARNESS_SCENARIO=api.backoff_503 .sdkharness/tests/run-resilience

# customer health (needs a bucket in the simulator first)
python - <<'PY'
import os
from b2sdk.v3 import B2Api, InMemoryAccountInfo
api = B2Api(InMemoryAccountInfo())
api.authorize_account('test-key-id', 'test-key', realm=os.environ['SDKHARNESS_SIMULATOR_URL'])
api.create_bucket('sdkharness-healthcheck', 'allPrivate')
PY
HEALTHCHECK_REALM_URL=$SDKHARNESS_SIMULATOR_URL B2_TEST_APPLICATION_KEY_ID=test-key-id \
  B2_TEST_APPLICATION_KEY=test-key B2_BUCKET_NAME=sdkharness-healthcheck \
  .sdkharness/tests/health-golden-path
```

Each prints one `SDKHARNESS_RESULT` line. Scenario ids are in `tests.tsv`.
A `FAIL` for `api.retry_after_503`, `upload.retry_408` is a known SDK finding, not a setup problem.

