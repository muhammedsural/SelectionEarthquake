from typing import Optional, Generic, TypeVar, Callable
from dataclasses import dataclass
from functools import wraps

# Type variables for generic programming
T = TypeVar('T')
E = TypeVar('E', bound=Exception)

# Result Pattern Implementation
@dataclass
class Result(Generic[T, E]):
    success: bool
    value: Optional[T] = None
    error: Optional[E] = None
    
    @classmethod
    def ok(cls, value: T) -> 'Result[T, E]':
        return cls(success=True, value=value)
    
    @classmethod
    def fail(cls, error: E) -> 'Result[T, E]':
        return cls(success=False, error=error)
    
    def unwrap(self) -> T:
        if self.success:
            return self.value
        raise self.error

# Decorators for error handling
def result_decorator(func: Callable[..., T]) -> Callable[..., Result[T, Exception]]:
    @wraps(func)
    def wrapper(*args, **kwargs) -> Result[T, Exception]:
        try:
            return Result.ok(func(*args, **kwargs))
        except Exception as e:
            return Result.fail(e)
    return wrapper

def async_result_decorator(func: Callable[..., T]) -> Callable[..., Result[T, Exception]]:
    @wraps(func)
    async def wrapper(*args, **kwargs) -> Result[T, Exception]:
        try:
            result = await func(*args, **kwargs)
            return Result.ok(result)
        except Exception as e:
            return Result.fail(e)
    return wrapper