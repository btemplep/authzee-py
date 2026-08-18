"""Paginators for {py:class}`authzee.authzee.Authzee` and {py:class}`authzee.authzee_async.AuthzeeAsync`.
"""

__all__ = [
    "paginator",
    "paginator_async"
]

from typing import Any, AsyncGenerator, Callable, Generator


def paginator(func: Callable, **kwargs) -> Generator[Any, None, None]:
    """Paginator for {py:class}`authzee.authzee.Authzee`.

    Parameters
    ----------
    func : Callable
        Method to paginate.
    **kwargs
        The KWArgs to pass to the method for pagination.

    Yields
    ------
    Generator[Any, None, None]
        The page of results

    Examples
    --------
    ```python
    from authzee import paginator

    # Assume authz is an Authzee instance
    for page in paginator(authz.list_grants):
        for grant in page['grants']:
            print(grant['grant_uuid'])
    ```
    """
    while True:
        result = func(**kwargs)

        yield result

        kwargs['page_ref'] = result['next_page_ref']
        if (
            result['next_page_ref'] is None
            or result['error'] is not None
        ):
            break


async def paginator_async(
    afunc: Callable,
    **kwargs
) -> AsyncGenerator[Any, None]:
    """Paginator for {py:class}`authzee.authzee_async.AuthzeeAsync`.

    Parameters
    ----------
    afunc : Callable
        Async method to paginate.
    **kwargs
        The KWArgs to pass to the method for pagination.

    Yields
    ------
    AsyncGenerator[Any, None]
        The page of results

    Examples
    --------
    ```python
    from authzee import paginator_async

    # Assume authz is an AuthzeeAsync instance, and this is within an event loop
    async for page in paginator_async(authz.list_grants):
        for grant in page['grants']:
            print(grant['grant_uuid'])
    ```
    """
    while True:
        result = await afunc(**kwargs)

        yield result

        kwargs['page_ref'] = result['next_page_ref']
        if (
            result['next_page_ref'] is None
            or result['error'] is not None
        ):
            break
