# Reviewer eval set (seed)

Past changes where a bug shipped or nearly shipped, for scoring an automated reviewer. Each case is a
diff the reviewer should flag. A maintainer who knows the code confirms the bug and where it lived before
a case counts. Re-score the reviewer against the confirmed set whenever `REVIEW.md`, `AGENTS.md`, the
model or the review workflow changes; keep a change only if the score does not drop. Add a case for every
revert or post-release fix PR.

Status: candidates, not yet confirmed.

| Case | Diff to review | Bug the reviewer should find | Fixed or caught in | Confirmed by |
| --- | --- | --- | --- | --- |
| Retry-After as HTTP date | the change that introduced `int(retry_after)` | `int()` raises `TypeError`/`ValueError` on a date-form `Retry-After` | #550 | |
| Retry in `post_content_return_json` | first version of #548 | a retry change in the shared POST path alters every POST, not only uploads | review of #548 | |
| Upload retried in HTTP layer | #615 | uploads retried with the same URL and token instead of in `UploadManager` | #618 | |
| v2 uses v3 abstractions | the change before #541 | an older apiver depends on v3-only classes | #541, #542 | |
| Simulator bucket mapping | the change before #543 | `RawSimulator` maps bucket names differently from B2 | #543 | |
| Positional misalignment | first version of #297 | new parameter inserted mid-signature shifts callers' positional arguments | review of #297 | |
| Upload option vs unfinished file | first version of #383 | a new upload option is ignored when matching an unfinished large file to resume | review of #383 | |
| Path traversal in sync | first version of #125 | `..` in a B2 file name escapes the destination folder | review of #125 | |
| Silent file drop in scan | first version of #390 | a symlink depth limit silently skips user files | review of #390 | |
| Harness reaches production | the change before #620 | the health check accepts ambient `B2_TEST_APPLICATION_KEY*` and could hit production | #620 | |

Also run the reviewer on known-clean merged PRs and count findings that would have led someone to make a
harmful change (the noise rate). Track rounds-to-pass alongside recall.
