# sdkharness contract

This directory exposes version-matched quality checks to the centralized
`sdkharness`. The repository owns the executable assertions; sdkharness owns the canonical
scenario, simulator, invocation, evidence, fleet report, and notification.

`tests.tsv` is the machine-readable entry point. Schema 1 has four
tab-separated columns: `test_level`, `scenario`, `target`, and `executable`.
Executables print one five-field, tab-separated result:

```text
SDKHARNESS_RESULT	health	golden-path	PASS	-
```

The customer-health, conformance, and resilience executables accept only
literal IPv4 loopback HTTP simulator URLs (`http://127.0.0.1:<port>`; `localhost`,
`::1` and DNS names are refused). They import `b2sdk` from this exact checkout
and never use a production B2 endpoint or real credentials: every check uses
only the fixed simulator credential `test-key-id` / `test-key`, and the health
check refuses any other `B2_TEST_APPLICATION_KEY*` pair before it reaches the
SDK. The individual conformance and resilience check files refuse to run at all
unless they are given a loopback simulator URL, so run them through the
dispatchers below, never with real `B2_*` values in the environment. The
dispatchers also drop `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`, `FTP_PROXY`,
`NO_PROXY` and `REQUEST_METHOD` (any case; `.sdkharness/tests/lib/loopback_guard.py`) and then pin
`NO_PROXY`/`no_proxy` to `127.0.0.1`, so a proxy configured on the machine cannot
change a result.

Conformance owns 33 capability checks and resilience owns 16 injected-fault
checks. Their small dispatchers validate the invocation, run the selected
repository-owned assertion, and translate its standing verdict into the
five-field `SDKHARNESS_RESULT` record. The central harness continues to own
scenario selection, simulator lifecycle, fleet evidence, issue reconciliation,
reporting, and notification.

## Run one check locally

Nothing here touches B2. You need this repository, **Python 3.10 or newer**
(older versions cannot install `b2sdk`), Node 20+ (for the simulator), and a
checkout of the standalone B2 simulator (available to the team; ask the SDK
harness owners for access). Set `B2SIM_DIR` to that checkout.

```bash
# 1. This checkout, in a virtualenv (the checks import b2sdk from here)
python -m venv .venv && . .venv/bin/activate && pip install -e .

# 2. A local simulator with the fault-control listener (--control)
export B2SIM_DIR=/path/to/your/simulator/checkout   # contains bin/simulator/serve.mjs
SIM_OUT=$(mktemp)
node "$B2SIM_DIR/bin/simulator/serve.mjs" --control > "$SIM_OUT" 2>&1 &    # prints the URLs
SIM_PID=$!

# 3. Wait until it has printed all three listener lines (http, https, control), then read them
for _ in $(seq 100); do
  [ "$(grep -c '^SIMULATOR-' "$SIM_OUT")" -ge 3 ] && break
  kill -0 "$SIM_PID" 2>/dev/null || { echo 'simulator exited:'; cat "$SIM_OUT"; break; }
  sleep 0.1
done
export SDKHARNESS_SIMULATOR_URL=$(sed -n 's/^SIMULATOR-LISTENING \(http:.*\)/\1/p' "$SIM_OUT")
export SDKHARNESS_SIMULATOR_HTTPS_URL=$(sed -n 's/^SIMULATOR-LISTENING \(https:.*\)/\1/p' "$SIM_OUT")
export SDKHARNESS_SIMULATOR_CONTROL_URL=$(sed -n 's/^SIMULATOR-CONTROL \(.*\)/\1/p' "$SIM_OUT")
export SDKHARNESS_SIMULATOR_CA=$B2SIM_DIR/bin/simulator/loopback-cert.pem
```

The standalone simulator also exports the same values as `B2SIM_URL`,
`B2SIM_HTTPS_URL`, `B2SIM_CONTROL_URL` and `B2SIM_CA` (its `bin/lib/simulator.sh`
helper); the checks read the `SDKHARNESS_SIMULATOR_*` names above.

Then run one scenario through its dispatcher. Each prints one
`SDKHARNESS_RESULT` line, for example
`SDKHARNESS_RESULT	conformance	files.upload	PASS	-`.

```bash
# conformance (one capability)
SDKHARNESS_TEST_LEVEL=conformance SDKHARNESS_SCENARIO=files.upload .sdkharness/tests/run-conformance

# resilience (one injected fault; needs the control URL, i.e. serve.mjs --control).
# Start a FRESH simulator for every scenario: the simulator keeps one request journal
# with no reset, the leaves count it, and the dispatcher FAILs a simulator that already
# served requests.
kill "$SIM_PID"; rm -f "$SIM_OUT"
# ...now repeat steps 2 and 3 to start a fresh simulator, then:
SDKHARNESS_TEST_LEVEL=resilience SDKHARNESS_SCENARIO=api.backoff_503 .sdkharness/tests/run-resilience

# customer health (needs a bucket in the simulator first, so use a fresh simulator again)
kill "$SIM_PID"; rm -f "$SIM_OUT"
# ...repeat steps 2 and 3 again, then:
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

When you are done, stop the simulator you started with `kill "$SIM_PID"` (never
kill by process name) and remove `"$SIM_OUT"`.

Scenario ids are in `tests.tsv`.
`api.retry_after_503` and `upload.retry_408` used to FAIL as known SDK findings;
both pass now, so there are no standing known findings listed here. `upload.stall`
reports `SKIP` because b2sdk documents no request-timeout option. A `FAIL` is a
real SDK or harness defect (or a simulator older than the checks expect), not a
setup problem. `SKIP` is reserved for a question that genuinely cannot be asked
(`no-realm-option`, `missing-runtime` when `b2sdk` is not installed at all,
`no-client-option`, `not-claimed`). An unreachable simulator, an authorization
error, an access-denied error, a connection error, or an SDK `ImportError` is
always a `FAIL`.
