import pytest

from b2sdk.filter import FilterMatcher, exclude, include


@pytest.mark.parametrize(
    ("filters", "expr", "expected"),
    (
        ([], "a", True),
        ([exclude("*")], "something", False),
        ([include("a-*")], "a-", True),
        ([include("a-*")], "b-", False),
        ([exclude("*.txt")], "a.txt", False),
        ([exclude("*.txt")], "a.csv", True),
        ([exclude("*"), include("*.[ct]sv")], "a.csv", True),
        ([exclude("*"), include("*.[ct]sv")], "a.tsv", True),
        ([exclude("*"), include("*.[ct]sv")], "a.ksv", False),
        ([exclude("*"), include("*.[ct]sv"), exclude("a.csv")], "a.csv", False),
        ([exclude("*"), include("*.[ct]sv"), exclude("a.csv")], "b.csv", True),
    ),
)
def test_filter_matcher(filters, expr, expected):
    assert FilterMatcher(filters).match(expr) == expected
