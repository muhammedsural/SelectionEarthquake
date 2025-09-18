from typing import Optional, Generic, TypeVar, Callable
from dataclasses import dataclass
from functools import wraps
import logging


logger = logging.getLogger(__name__)

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

    def __repr__(self):
        if self.success:
            return f"<Result OK value={type(self.value)}>"
        return f"<Result FAIL error={self.error}>"
    
# Decorators for error handling
def result_decorator(func: Callable[..., T]) -> Callable[..., Result[T, Exception]]:
    @wraps(func)
    def wrapper(*args, **kwargs) -> Result[T, Exception]:
        func_name = func.__qualname__
        logger.debug("[DEBUG] %s called (args=%s kwargs=%s)", func_name, args[1:], kwargs)
        try:
            result = func(*args, **kwargs)
            logger.info("[OK] %s completed successfully", func_name)
            return Result.ok(result)
        except Exception as e:
            logger.error("[ERROR] %s failed: %s", func_name, e, exc_info=True)
            return Result.fail(e)
    return wrapper

def async_result_decorator(func: Callable[..., T]) -> Callable[..., Result[T, Exception]]:
    @wraps(func)
    async def wrapper(*args, **kwargs) -> Result[T, Exception]:
        func_name = func.__qualname__
        logger.debug("[DEBUG] [async] %s called (args=%s kwargs=%s)", func_name, args[1:], kwargs)
        try:
            result = await func(*args, **kwargs)
            logger.info("[OK] [async] %s completed successfully", func_name)
            return Result.ok(result)
        except Exception as e:
            logger.error("[ERROR] [async] %s failed: %s", func_name, e, exc_info=True)
            return Result.fail(e)
    return wrapper