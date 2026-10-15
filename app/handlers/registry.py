from app.exceptions.errors import UnknownJobTypeError
from app.handlers.base import BaseJobHandler


class HandlerRegistry:
    """Thread-safe registry mapping job_type strings to concrete handler instances."""

    def __init__(self) -> None:
        self._handlers: dict[str, BaseJobHandler] = {}

    def register(self, handler_cls: type[BaseJobHandler]) -> type[BaseJobHandler]:
        """Register a handler class instance."""
        if not hasattr(handler_cls, "job_type") or not handler_cls.job_type:
            raise ValueError(f"Handler {handler_cls} must define a non-empty 'job_type'")
        if not hasattr(handler_cls, "payload_schema"):
            raise ValueError(f"Handler {handler_cls} must define 'payload_schema'")

        instance = handler_cls()
        self._handlers[handler_cls.job_type] = instance
        return handler_cls

    def get(self, job_type: str) -> BaseJobHandler:
        """Retrieve handler instance for the given job type."""
        handler = self._handlers.get(job_type)
        if handler is None:
            raise UnknownJobTypeError(job_type)
        return handler

    def is_registered(self, job_type: str) -> bool:
        """Check if a handler exists for the given job type."""
        return job_type in self._handlers

    def list_job_types(self) -> list[str]:
        """List all currently registered job types."""
        return sorted(self._handlers.keys())


# Global registry singleton
registry = HandlerRegistry()


def register_handler(handler_cls: type[BaseJobHandler]) -> type[BaseJobHandler]:
    """Decorator for registering job handlers."""
    return registry.register(handler_cls)
