class BaseTaskiqPSQLPyError(Exception):
    """Base error for all possible exception in the lib."""


class ResultIsMissingError(BaseTaskiqPSQLPyError):
    """Error if cannot retrieve result from PostgreSQL."""
