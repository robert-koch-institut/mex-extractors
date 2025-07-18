import pytest

from mex.extractors.utils import ensure_list


@pytest.mark.parametrize(
    ("input_value", "expected"),
    [
        (None, []),
        ([], []),
        ([1, 2, 3], [1, 2, 3]),
        ("hello", ["hello"]),
        ({"key": "value"}, [{"key": "value"}]),
    ],
)
def test_ensure_list(input_value: object, expected: list[object]) -> None:
    result = ensure_list(input_value)
    assert result == expected
