"""JMESPath custom function provided by Authzee.

See [](authzee.jmespath.CustomJMESPathFunctions)
"""

__all__ = [
    "CustomJMESPathFunctions",
    "jmespath_custom_execute",
    "jmespath_execute"
]

import re
from typing import Any, Dict, List, Union


try:
    from jmespath import exceptions, functions, Options, search
except ModuleNotFoundError: # pragma: no cover
    pass


class CustomJMESPathFunctions(functions.Functions):
    """JMESPath custom functions.

    Along with the standard [Built-in JMESPath Functions](https://jmespath.org/specification.html#built-in-functions)
    the following custom functions are added"

    - `array[object] inner_join(array[any] $lhs, array[any] $rhs, expression->boolean expr)`
        - Like an SQL INNER JOIN
        - See [SDK Docs INNER JOIN](https://github.com/btemplep/authzee/blob/main/docs/sdks.md#inner-join)
    - `array[object] left_join(array[any] $lhs, array[any] $rhs, expression->boolean expr)`
        - Like an SQL LEFT JOIN
        - See [SDK Docs LEFT JOIN](https://github.com/btemplep/authzee/blob/main/docs/sdks.md#left-join)
    - `array[object] outer_join(array[any] $lhs, array[any] $rhs, expression->boolean expr)`
        - Like an SQL OUTER JOIN
        - See [SDK Docs OUTER JOIN](https://github.com/btemplep/authzee/blob/main/docs/sdks.md#outer-join)
    - `boolean is_identity_present(string $itype, object $request)`
        - Checks if at least one entry of the specified identity type is present in the request
        - Returns true if present, or else false
        - See [SDK Docs Is Identity Present](https://github.com/btemplep/authzee/blob/main/docs/sdks.md#is-identity-present)
    - `string|null|array[string|null] regex_find(string $pattern, string|array[string] $subject)`
        - The return value depends on the subject type:
            - `string` - Run a regex pattern against a string and return the first occurrence of the pattern or `null` if there are none.
            - `array[string]` - Run a regex pattern on an array of strings and return an equal length array where each element is the first occurrence of the pattern or `null` if there are none.
        - See [SDK Docs regex Find](https://github.com/btemplep/authzee/blob/main/docs/sdks.md#regex-find)
    - `array[string]|array[array[string]] regex_find_all(string $pattern, string|array[string] $subject)`
        - The return value depends on the subject type:
            - `string` - Run a regex pattern against a string and return an array of all occurrences of the pattern in the string.
            - `array[string]` - Run a regex pattern on an array of strings and return an equal length array of results where each element is an array of all occurrences of the pattern in the string.
        - See [SDK Docs regex Find All](https://github.com/btemplep/authzee/blob/main/docs/sdks.md#regex-find-all)
    - `null|array[string|null]|array[array[string|null]|null] regex_groups(string|array[string] $subject, string $pattern)`
        - The return value depends on the subject type:
            - `string` - Run a regex pattern against a string and return an array of all groups from the first occurrence of the pattern, or `null` if there are no pattern matches. If a group has no value it will be `null`.
            - `array[string]` - Run a regex pattern on an array of strings and return an equal length array where each element is an array of groups from the first occurrence of the pattern or `null` if there are no pattern matches. If a group has no value it will be `null`.
        - See [SDK Docs regex Groups](https://github.com/btemplep/authzee/blob/main/docs/sdks.md#regex-groups)
    - `array[array[string|null]]|array[array[array[string|null]]] regex_groups_all(string|array[string] $subject, string $pattern)`
        - The return value depends on the subject type:
            - `string` - Run a regex pattern against a string and return an array where each item is an array of groups for each occurrence of the pattern. If a group has no value it will be `null`.
            - `array[string]` - Run a regex pattern on an array of strings and return an equal length array where each element is an array of all occurrences of the pattern.  Each element in the array of occurrences is an array of the groups. If a group has no value it will be `null`.
        - See [SDK Docs regex Groups All](https://github.com/btemplep/authzee/blob/main/docs/sdks.md#regex-groups-all)
    - `string lower(string $subject)`
        - Convert string to lowercase.
        - See [SDK Docs String Lower](https://github.com/btemplep/authzee/blob/main/docs/sdks.md#string-lower)
    - `string upper(string $subject)`
        - Convert string to uppercase.
        - See [SDK Docs String Upper](https://github.com/btemplep/authzee/blob/main/docs/sdks.md#string-upper)
    """


    def __init__(self):
        super().__init__()
        self._custom_options = Options(custom_functions=self)


    @functions.signature(
        {
            "types": [
                "array"
            ]
        },
        {
            "types": [
                "array"
            ]
        },
        {
            "types": [
                "string"
            ]
        }
    )
    def _func_inner_join(
        self,
        lhs: List[Any],
        rhs: List[Any],
        expr: str
    ) -> List[Dict[str, Any]]:
        result = []
        for l in lhs:
            for r in rhs:
                # expref.visit(expref.expression, element) # this is how they do it internal to jmespath python??
                if (
                    search(
                        expr,
                        {
                            "lhs": l,
                            "rhs": r
                        },
                        options=self._custom_options
                    )
                    is True
                ):
                    result.append(
                        {
                            "lhs": l,
                            "rhs": r
                        }
                    )

        return result


    @functions.signature(
        {
            "types": [
                "array"
            ]
        },
        {
            "types": [
                "array"
            ]
        },
        {
            "types": [
                "string"
            ]
        }
    )
    def _func_left_join(
        self,
        lhs: List[Any],
        rhs: List[Any],
        expr: str
    ) -> List[Dict[str, Any]]:
        result = []
        for l in lhs:
            lhs_match = False
            for r in rhs:
                if search( # Should use jmespath search function set in Authzee.
                    expr,
                    {
                        "lhs": l,
                        "rhs": r
                    },
                    options=self._custom_options
                ) is True:
                    lhs_match = True
                    result.append(
                        {
                            "lhs": l,
                            "rhs": r
                        }
                    )

            if lhs_match is False:
                result.append(
                    {
                        "lhs": l,
                        "rhs": None
                    }
                )

        return result


    @functions.signature(
        {
            "types": [
                "array"
            ]
        },
        {
            "types": [
                "array"
            ]
        },
        {
            "types": [
                "string"
            ]
        }
    )
    def _func_outer_join(
        self,
        lhs: List[Any],
        rhs: List[Any],
        expr: str
    ) -> List[Dict[str, Any]]:
        result = []
        unmatched_rhs = set(rhs)
        for l in lhs:
            lhs_match = False
            for r in rhs:
                if search( # Should use jmespath search function set in Authzee.
                    expr,
                    {
                        "lhs": l,
                        "rhs": r
                    },
                    options=self._custom_options
                ) is True:
                    unmatched_rhs.discard(r)
                    lhs_match = True
                    result.append(
                        {
                            "lhs": l,
                            "rhs": r
                        }
                    )

            if lhs_match is False:
                result.append(
                    {
                        "lhs": l,
                        "rhs": None
                    }
                )

        for r in unmatched_rhs:
            result.append(
                {
                    "lhs": None,
                    "rhs": r
                }
            )

        return result


    @functions.signature(
        {
            "types": [
                "string"
            ]
        },
        {
            "types": [
                "object"
            ]
        }
    )
    def _func_is_identity_present(itype: str, request: dict) -> bool:
        if (
            itype in request['identities']
            and len(request['identities'][itype]) > 0
        ):
            return True

        return False


    @functions.signature(
        {
            "types": [
                "string"
            ]
        },
        {
            "types": [
                "string",
                "array-string"
            ]
        }
    )
    def _func_regex_find(
        pattern: str,
        subject: Union[str, List[str]]
    ) -> Union[
        None,
        str,
        List[Union[None, str]]
    ]:
        if type(subject) is str:
            match = re.search(pattern, subject)
            if match is not None:
                return match.group()

            else:
                return None

        if type(subject) is list:
            result = []
            for sub in subject:
                match = re.search(pattern, sub)
                if match is not None:
                    result.append(match.group())
                else:
                    result.append(None)

        return result


    @functions.signature(
        {
            "types": [
                "string"
            ]
        },
        {
            "types": [
                "string",
                "array-string"
            ]
        }
    )
    def _func_regex_find_all(
        pattern: str,
        subject: Union[str, List[str]]
    ) -> Union[
        List[str],
        List[List[str]]
    ]:
        if type(subject) is str:
            return re.findall(pattern, subject)

        if type(subject) is list:
            result = []
            for sub in subject:
                result.append(re.findall(pattern, sub))

        return result


    @functions.signature(
        {
            "types": [
                "string"
            ]
        },
        {
            "types": [
                "string",
                "array-string"
            ]
        }
    )
    def _func_regex_groups(
        pattern: str,
        subject: Union[str, List[str]]
    ) -> Union[
        None,
        List[Union[None, str]],
        List[
            Union[
                None,
                List[Union[None, str]]
            ]
        ]
    ]:
        if type(subject) is str:
            match = re.search(pattern, subject)
            if match is not None:
                return list(match.groups())

            else:
                return None

        if type(subject) is list:
            result = []
            for sub in subject:
                match = re.search(pattern, sub)
                if match is not None:
                    result.append(list(match.groups()))
                else:
                    result.append(None)

        return result


    @functions.signature(
        {
            "types": [
                "string"
            ]
        },
        {
            "types": [
                "string",
                "array-string"
            ]
        }
    )
    def _func_regex_groups_all(
        pattern: str,
        subject: Union[str, List[str]]
    ) -> Union[
        List[str],
        List[List[str]]
    ]:
        if type(subject) is str:
            return [list(m.groups()) if m is not None else None for m in re.finditer(pattern, subject)]

        if type(subject) is list:
            result = []
            for sub in subject:
                result.append(
                    [list(m.groups()) if m is not None else None for m in re.finditer(pattern, sub)]
                )

        return result


    @functions.signature(
        {
            "types": [
                "string"
            ]
        }
    )
    def _func_lower(self, string: str) -> str:
        return string.lower()


    @functions.signature(
        {
            "types": [
                "string"
            ]
        }
    )
    def _func_upper(self, string: str) -> str:
        return string.upper()


def jmespath_execute(expression: str, data: Any) -> dict:
    """Standard JMESPath JSON execute function for Authzee.

    See the standard [Built-in JMESPath Functions](https://jmespath.org/specification.html#built-in-functions).
    """
    query_result = None
    try:
        query_result = search(expression, data)
    except Exception as exc:
        return {
            "result": None,
            "failure": f"A JMESPath Query error has occurred. [{exc.__class__.__qualname__}] - {exc}"
        }

    return {
        "result": query_result,
        "failure": None
    }


_custom_options = Options(custom_functions=CustomJMESPathFunctions())


def jmespath_custom_execute(expression: str, data: Any) -> dict:
    """Standard JMESPath JSON execute function for Authzee that includes SDK recommended custom functions:

    Along with the standard [Built-in JMESPath Functions](https://jmespath.org/specification.html#built-in-functions)
    the following custom functions are added"

    - `array[object] inner_join(array[any] $lhs, array[any] $rhs, expression->boolean expr)`
        - Like an SQL INNER JOIN
        - See [SDK Docs INNER JOIN](https://github.com/btemplep/authzee/blob/main/docs/sdks.md#inner-join)
    - `array[object] left_join(array[any] $lhs, array[any] $rhs, expression->boolean expr)`
        - Like an SQL LEFT JOIN
        - See [SDK Docs LEFT JOIN](https://github.com/btemplep/authzee/blob/main/docs/sdks.md#left-join)
    - `array[object] outer_join(array[any] $lhs, array[any] $rhs, expression->boolean expr)`
        - Like an SQL OUTER JOIN
        - See [SDK Docs OUTER JOIN](https://github.com/btemplep/authzee/blob/main/docs/sdks.md#outer-join)
    - `boolean is_identity_present(string $itype, object $request)`
        - Checks if at least one entry of the specified identity type is present in the request
        - Returns true if present, or else false
        - See [SDK Docs Is Identity Present](https://github.com/btemplep/authzee/blob/main/docs/sdks.md#is-identity-present)
    - `string|null|array[string|null] regex_find(string $pattern, string|array[string] $subject)`
        - The return value depends on the subject type:
            - `string` - Run a regex pattern against a string and return the first occurrence of the pattern or `null` if there are none.
            - `array[string]` - Run a regex pattern on an array of strings and return an equal length array where each element is the first occurrence of the pattern or `null` if there are none.
        - See [SDK Docs regex Find](https://github.com/btemplep/authzee/blob/main/docs/sdks.md#regex-find)
    - `array[string]|array[array[string]] regex_find_all(string $pattern, string|array[string] $subject)`
        - The return value depends on the subject type:
            - `string` - Run a regex pattern against a string and return an array of all occurrences of the pattern in the string.
            - `array[string]` - Run a regex pattern on an array of strings and return an equal length array of results where each element is an array of all occurrences of the pattern in the string.
        - See [SDK Docs regex Find All](https://github.com/btemplep/authzee/blob/main/docs/sdks.md#regex-find-all)
    - `null|array[string|null]|array[array[string|null]|null] regex_groups(string|array[string] $subject, string $pattern)`
        - The return value depends on the subject type:
            - `string` - Run a regex pattern against a string and return an array of all groups from the first occurrence of the pattern, or `null` if there are no pattern matches. If a group has no value it will be `null`.
            - `array[string]` - Run a regex pattern on an array of strings and return an equal length array where each element is an array of groups from the first occurrence of the pattern or `null` if there are no pattern matches. If a group has no value it will be `null`.
        - See [SDK Docs regex Groups](https://github.com/btemplep/authzee/blob/main/docs/sdks.md#regex-groups)
    - `array[array[string|null]]|array[array[array[string|null]]] regex_groups_all(string|array[string] $subject, string $pattern)`
        - The return value depends on the subject type:
            - `string` - Run a regex pattern against a string and return an array where each item is an array of groups for each occurrence of the pattern. If a group has no value it will be `null`.
            - `array[string]` - Run a regex pattern on an array of strings and return an equal length array where each element is an array of all occurrences of the pattern.  Each element in the array of occurrences is an array of the groups. If a group has no value it will be `null`.
        - See [SDK Docs regex Groups All](https://github.com/btemplep/authzee/blob/main/docs/sdks.md#regex-groups-all)
    - `string lower(string $subject)`
        - Convert string to lowercase.
        - See [SDK Docs String Lower](https://github.com/btemplep/authzee/blob/main/docs/sdks.md#string-lower)
    - `string upper(string $subject)`
        - Convert string to uppercase.
        - See [SDK Docs String Upper](https://github.com/btemplep/authzee/blob/main/docs/sdks.md#string-upper)
    """
    query_result = None
    try:
        query_result = search(
            expression,
            data,
            options=_custom_options
        )
    except Exception as exc:
        return {
            "result": None,
            "failure": f"A JMESPath Query error has occurred. [{exc.__class__.__qualname__}] - {exc}"
        }

    return {
        "result": query_result,
        "failure": None
    }
