
from typing import Any, AsyncGenerator, Callable, Generator


def paginator(func: Callable, **kwargs) -> Generator[Any, None]:
    while True:
        result = func(**kwargs)

        yield result

        kwargs['page_ref'] = result['next_page_ref']
        if result['next_page_ref'] is None or result['has_failed'] is True:
            break


async def paginator_async(afunc: Callable, **kwargs) -> AsyncGenerator[Any, None]:
    while True:
        result = await afunc(**kwargs)
        
        yield result

        kwargs['page_ref'] = result['next_page_ref']
        if result['next_page_ref'] is None or result['has_failed'] is True:
            break
