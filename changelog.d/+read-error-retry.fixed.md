Retry stream read errors during download in `SimpleDownloader`.

Decoded downloads with `decode_content=True` now validate truncation; previously all post-download checks were skipped for decoded streams.

Fix `b2sdk.v1.B2Api` not exposing `api_config`.
