######################################################################
#
# File: contrib/review_metrics.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""
Report how much human review merged PRs got, and how often changes were reverted.

Usage: python contrib/review_metrics.py [--repo OWNER/NAME] [--days 30]

Needs an authenticated `gh` CLI. Read-only. Bots (logins ending in `[bot]` or starting with
`copilot`) never count as human reviewers; the PR author never counts as their own reviewer. Only
reviews and comments made before the merge count; replies inside review threads count.
"""

from __future__ import annotations

import argparse
import datetime
import json
import statistics
import subprocess

QUERY = """
query($q: String!, $endCursor: String) {
  search(query: $q, type: ISSUE, first: 20, after: $endCursor) {
    pageInfo { hasNextPage endCursor }
    nodes {
      ... on PullRequest {
        number title createdAt mergedAt author { login }
        reviews(first: 100) { totalCount nodes { author { login } state submittedAt } }
        comments(first: 100) { totalCount nodes { author { login } createdAt } }
        reviewThreads(first: 100) {
          totalCount
          nodes { comments(first: 50) { totalCount nodes { author { login } createdAt } } }
        }
      }
    }
  }
}
"""


def _is_bot(login: str | None) -> bool:
    return not login or login.endswith('[bot]') or login.lower().startswith('copilot')


def _parse_time(value: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(value.replace('Z', '+00:00'))


def _fetch(repo: str, days: int) -> list[dict]:
    since = (datetime.date.today() - datetime.timedelta(days=days)).isoformat()
    search = f'repo:{repo} is:pr is:merged merged:>={since}'
    result = subprocess.run(
        ['gh', 'api', 'graphql', '--paginate', '-f', f'query={QUERY}', '-f', f'q={search}'],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode:
        raise SystemExit(f'gh api graphql failed: {result.stderr.strip()}')
    out = result.stdout
    decoder = json.JSONDecoder()
    pages, index = [], 0
    while index < len(out):
        if out[index].isspace():
            index += 1
            continue
        page, index = decoder.raw_decode(out, index)
        pages.append(page)
    return [node for page in pages for node in page['data']['search']['nodes'] if node]


def _human_before_merge(
    nodes: list[dict], author: str | None, merged_at, time_key: str
) -> list[dict]:
    """Keep items by people other than the author, submitted before the PR merged (pending reviews,
    which have no timestamp, are dropped)."""
    return [
        node
        for node in nodes
        if not _is_bot((node['author'] or {}).get('login'))
        and node['author']['login'] != author
        and node[time_key]
        and _parse_time(node[time_key]) <= merged_at
    ]


def _truncated(pr: dict) -> bool:
    connections = [pr['reviews'], pr['comments'], pr['reviewThreads']]
    connections += [thread['comments'] for thread in pr['reviewThreads']['nodes']]
    return any(c['totalCount'] > len(c['nodes']) for c in connections)


def _percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(fraction * len(ordered)))]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument('--repo', default='Backblaze/b2-sdk-python')
    parser.add_argument('--days', type=int, default=30)
    args = parser.parse_args()

    prs = _fetch(args.repo, args.days)
    if not prs:
        print(f'No PRs merged in {args.repo} in the last {args.days} days.')
        return

    approvals, comments, waits, rounds = [], [], [], []
    unreviewed = 0
    truncated = [pr['number'] for pr in prs if _truncated(pr)]
    for pr in prs:
        author = (pr['author'] or {}).get('login')
        merged_at = _parse_time(pr['mergedAt'])
        reviews = _human_before_merge(pr['reviews']['nodes'], author, merged_at, 'submittedAt')
        approvers = {r['author']['login'] for r in reviews if r['state'] == 'APPROVED'}
        thread_comments = [c for t in pr['reviewThreads']['nodes'] for c in t['comments']['nodes']]
        human_comments = _human_before_merge(
            pr['comments']['nodes'] + thread_comments, author, merged_at, 'createdAt'
        )
        approvals.append(len(approvers))
        comments.append(len(human_comments))
        rounds.append(len(reviews))
        if reviews:
            first = min(_parse_time(r['submittedAt']) for r in reviews)
            waits.append((first - _parse_time(pr['createdAt'])).total_seconds() / 3600)
        else:
            unreviewed += 1

    reverts = [pr for pr in prs if pr['title'].lower().startswith('revert')]
    print(f'{args.repo}: {len(prs)} PRs merged in the last {args.days} days')
    print(
        f'  human approvals per PR: median {statistics.median(approvals)}, '
        f'p95 {_percentile(approvals, 0.95)}'
    )
    print(f'  human comments per PR: median {statistics.median(comments)}')
    print(f'  human review submissions per PR (rounds): mean {statistics.mean(rounds):.2f}')
    print(f'  merged with no human review: {unreviewed} ({unreviewed / len(prs):.0%})')
    if waits:
        print(
            f'  hours to first human review: median {statistics.median(waits):.1f}, '
            f'p95 {_percentile(waits, 0.95):.1f}'
        )
    print(f'  reverts (guardrail): {len(reverts)} {[pr["number"] for pr in reverts]}')
    if truncated:
        print(
            f'  note: more than 100 reviews, comments or threads; counts are lower bounds on {truncated}'
        )


if __name__ == '__main__':
    main()
