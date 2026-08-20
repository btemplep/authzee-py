"""Unit tests for authzee.jmespath module."""

import pytest

from authzee.jmespath import (
    CustomJMESPathFunctions,
    jmespath_custom_execute,
    jmespath_execute
)


def test_jmespath_execute_simple_expression():
    result = jmespath_execute(
        "a",
        {
            "a": 1,
            "b": 2
        }
    )
    assert result == {
        "result": 1,
        "failure": None
    }


def test_jmespath_execute_returns_none_for_missing_key():
    result = jmespath_execute(
        "z",
        {
            "a": 1
        }
    )
    assert result['result'] is None
    assert result['failure'] is None


def test_jmespath_execute_invalid_expression():
    result = jmespath_execute(
        "a.[invalid",
        {
            "a": 1
        }
    )
    assert result['failure'] is not None
    assert result['result'] is None
    assert "JMESPath Query error" in result['failure']


def test_jmespath_execute_nested():
    data = {
        "a": {
            "b": {
                "c": 42
            }
        }
    }
    result = jmespath_execute("a.b.c", data)
    assert result['result'] == 42
    assert result['failure'] is None


def test_jmespath_custom_execute_simple():
    result = jmespath_custom_execute(
        "a",
        {
            "a": "hello"
        }
    )
    assert result == {
        "result": "hello",
        "failure": None
    }


def test_jmespath_custom_execute_invalid_expression():
    result = jmespath_custom_execute(
        "a.[invalid",
        {
            "a": 1
        }
    )
    assert result['failure'] is not None
    assert result['result'] is None
    assert "JMESPath Query error" in result['failure']


def test_custom_lower():
    result = jmespath_custom_execute("lower('HELLO')", {})
    assert result['result'] == "hello"


def test_custom_upper():
    result = jmespath_custom_execute("upper('hello')", {})
    assert result['result'] == "HELLO"


def test_inner_join_basic():
    data = {
        "lhs_arr": [
            1,
            2,
            3
        ],
        "rhs_arr": [
            2,
            3,
            4
        ]
    }
    result = jmespath_custom_execute(
        "inner_join(lhs_arr, rhs_arr, 'lhs == rhs')",
        data
    )
    assert result['failure'] is None
    joined = result['result']
    assert len(joined) == 2
    assert {
        "lhs": 2,
        "rhs": 2
    } in joined
    assert {
        "lhs": 3,
        "rhs": 3
    } in joined


def test_inner_join_no_matches():
    data = {
        "lhs_arr": [
            1
        ],
        "rhs_arr": [
            2
        ]
    }
    result = jmespath_custom_execute(
        "inner_join(lhs_arr, rhs_arr, 'lhs == rhs')",
        data
    )
    assert result['failure'] is None
    assert result['result'] == []


def test_left_join_basic():
    data = {
        "lhs_arr": [
            1,
            2,
            3
        ],
        "rhs_arr": [
            2,
            3,
            4
        ]
    }
    result = jmespath_custom_execute(
        "left_join(lhs_arr, rhs_arr, 'lhs == rhs')",
        data
    )
    assert result['failure'] is None
    joined = result['result']
    assert {
        "lhs": 1,
        "rhs": None
    } in joined
    assert {
        "lhs": 2,
        "rhs": 2
    } in joined
    assert {
        "lhs": 3,
        "rhs": 3
    } in joined


def test_left_join_no_rhs_matches():
    data = {
        "lhs_arr": [
            1,
            2
        ],
        "rhs_arr": [
            5
        ]
    }
    result = jmespath_custom_execute(
        "left_join(lhs_arr, rhs_arr, 'lhs == rhs')",
        data
    )
    assert result['failure'] is None
    joined = result['result']
    assert {
        "lhs": 1,
        "rhs": None
    } in joined
    assert {
        "lhs": 2,
        "rhs": None
    } in joined


def test_outer_join_basic():
    data = {
        "lhs_arr": [
            1,
            2
        ],
        "rhs_arr": [
            2,
            3
        ]
    }
    result = jmespath_custom_execute(
        "outer_join(lhs_arr, rhs_arr, 'lhs == rhs')",
        data
    )
    assert result['failure'] is None
    joined = result['result']
    assert {
        "lhs": 1,
        "rhs": None
    } in joined
    assert {
        "lhs": 2,
        "rhs": 2
    } in joined
    assert {
        "lhs": None,
        "rhs": 3
    } in joined


def test_regex_find_direct_string_match():
    result = CustomJMESPathFunctions._func_regex_find(
        "\\d+",
        "hello 123 world"
    )
    assert result == "123"


def test_regex_find_direct_string_no_match():
    result = CustomJMESPathFunctions._func_regex_find(
        "\\d+",
        "hello world"
    )
    assert result is None


def test_regex_find_direct_array():
    result = CustomJMESPathFunctions._func_regex_find(
        "\\d+",
        ["abc123", "def", "456ghi"]
    )
    assert result == ["123", None, "456"]


def test_regex_find_all_direct_string():
    result = CustomJMESPathFunctions._func_regex_find_all(
        "\\d",
        "a1b2c3"
    )
    assert result == ["1", "2", "3"]


def test_regex_find_all_direct_array():
    result = CustomJMESPathFunctions._func_regex_find_all(
        "\\d",
        ["a1b2", "c3"]
    )
    assert result == [
        [
            "1",
            "2"
        ],
        [
            "3"
        ]
    ]


def test_regex_groups_direct_string_match():
    result = CustomJMESPathFunctions._func_regex_groups(
        "(\\d{4})-(\\d{2})-(\\d{2})",
        "2024-01-15"
    )
    assert result == ["2024", "01", "15"]


def test_regex_groups_direct_string_no_match():
    result = CustomJMESPathFunctions._func_regex_groups(
        "(\\d{4})-(\\d{2})",
        "no date"
    )
    assert result is None


def test_regex_groups_direct_array():
    result = CustomJMESPathFunctions._func_regex_groups(
        "(\\d+)",
        ["abc123", "def"]
    )
    assert result == [
        [
            "123"
        ],
        None
    ]


def test_regex_groups_all_direct_string():
    result = CustomJMESPathFunctions._func_regex_groups_all(
        "([a-z])(\\d)",
        "a1b2c3"
    )
    assert result == [
        [
            "a",
            "1"
        ],
        [
            "b",
            "2"
        ],
        [
            "c",
            "3"
        ]
    ]


def test_regex_groups_all_direct_array():
    result = CustomJMESPathFunctions._func_regex_groups_all(
        "([a-z])(\\d)",
        ["a1b2", "c3"]
    )
    assert result == [
        [
            [
                "a",
                "1"
            ],
            [
                "b",
                "2"
            ]
        ],
        [
            [
                "c",
                "3"
            ]
        ]
    ]


def test_is_identity_present_true():
    data = {
        "itype": "user",
        "request": {
            "identities": {
                "user": [
                    {
                        "name": "test"
                    }
                ]
            }
        }
    }
    result = jmespath_custom_execute(
        "is_identity_present(itype, request)",
        data
    )
    if result['failure'] is None:
        assert result['result'] is True


def test_is_identity_present_false():
    data = {
        "itype": "admin",
        "request": {
            "identities": {
                "user": [
                    {
                        "name": "test"
                    }
                ]
            }
        }
    }
    result = jmespath_custom_execute(
        "is_identity_present(itype, request)",
        data
    )
    if result['failure'] is None:
        assert result['result'] is False


def test_is_identity_present_direct():
    result = CustomJMESPathFunctions._func_is_identity_present(
        "user",
        {
            "identities": {
                "user": [
                    {
                        "name": "test"
                    }
                ]
            }
        }
    )
    assert result is True

    result = CustomJMESPathFunctions._func_is_identity_present(
        "admin",
        {
            "identities": {
                "user": [
                    {
                        "name": "test"
                    }
                ]
            }
        }
    )
    assert result is False

    result = CustomJMESPathFunctions._func_is_identity_present(
        "user",
        {
            "identities": {
                "user": []
            }
        }
    )
    assert result is False
