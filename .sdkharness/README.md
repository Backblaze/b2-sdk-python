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
checkout and never use a production B2 endpoint or real credentials.

Conformance owns 33 capability checks and resilience owns 16 injected-fault
checks. Their small dispatchers validate the invocation, run the selected
repository-owned assertion, and translate its standing verdict into the
five-field `SDKHARNESS_RESULT` record. The central harness continues to own
scenario selection, simulator lifecycle, fleet evidence, issue reconciliation,
reporting, and notification.
