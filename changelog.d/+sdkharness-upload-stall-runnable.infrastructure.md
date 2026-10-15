Make the sdkharness `upload.stall` check runnable: it meets its request-timeout need through the documented `B2HttpApiConfig(http_session_factory=...)` instead of reporting `no-client-option`.
