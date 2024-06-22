# Copyright 2023 Broda Group Software Inc.
#
# Created: 2023-07-06 by eric.broda@brodagrouopsoftware.com

class BgsException(Exception):
    """
    General exception
    """

    def __init__(self, message, original_exception=None):
        super().__init__(message)
        self.original_exception = original_exception


class BgsNotFoundException(BgsException):
    """
    Not Found Exception
    """

    def __init__(self, message, original_exception=None):
        super().__init__(message)
        self.original_exception = original_exception


class BgsAlreadyExistsException(BgsException):
    """
    Already Exists Exception
    """

    def __init__(self, message, original_exception=None):
        super().__init__(message)
        self.original_exception = original_exception


class InvalidLatLongException(BgsException):
    """
    Exception to be used when latitude or longitude is not
    a valid value.

    Latitude must be between [-90, 90]
    Longitude must be between [-180, 180]
    """

    def __init__(self, message, original_exception=None):
        super().__init__(message)
        self.original_exception = original_exception


class DBDirNotExistsException(BgsException):
    """
    Exception to be thrown if the database directory does not exist
    """

    def __init__(self, message, original_exception=None):
        super().__init__(message)
        self.original_exception = original_exception


class DataSetNotRegisteredException(BgsException):
    """
    Exception raised if a dataset is not registered in the metadata
    """

    def __init__(self, message, original_exception=None):
        super().__init__(message)
        self.original_exception = original_exception


class InvalidColumnTypeException(BgsException):
    """
    Exception raised if a column type provided is not valid
    """

    def __init__(self, message, original_exception=None):
        super().__init__(message)
        self.original_exception = original_exception


class IntervalInvalidException(BgsException):
    """
    Thrown when an interval in the database is not valid.
    For instance, when a month is specified in a yearly dataset,
    or when a day is not specified in a daily dataset.
    """

    def __init__(self, message, original_exception=None):
        super().__init__(message)
        self.original_exception = original_exception


class InvalidCharacterException(BgsException):
    """
    Thrown when something has a limited set of chracters that are
    acceptable, and a character outside that set is provided.
    """

    def __init__(self, message, original_exception=None):
        super().__init__(message)
        self.original_exception = original_exception


class InvalidArgumentException(BgsException):
    """
    Raised whenever an argument is not valid, and there
    is no more specific exception to raise.
    """

    def __init__(self, message, original_exception=None):
        super().__init__(message)
        self.original_exception = original_exception


class OperationUnsupportedException(BgsException):
    """
    Raised when an operation is not supported, and no
    more specific exception is available
    """

    def __init__(self, message, original_exception=None):
        super().__init__(message)
        self.original_exception = original_exception


class WrongColumnNumberException(BgsException):
    """
    raised when the number of columns is not the
    expected number of columns
    """

    def __init__(self, message, original_exception=None):
        super().__init__(message)
        self.original_exception = original_exception

class InvalidConfException(BgsException):
    """
    raised when a provided configuration is not valid
    """

    def __init__(self, message, original_exception=None):
        super().__init__(message)
        self.original_exception = original_exception

class WrongFileTypeException(BgsException):
    """
    raised when a file is not of correct type
    """

    def __init__(self, message, original_exception=None):
        super().__init__(message)
        self.original_exception = original_exception

class ConfigException(BgsException):
    """
    Raised when something is wrong with a config, and
    no more specific exception is available
    """
    def __init__(self, message, original_exception=None):
        super().__init__(message)
        self.original_exception = original_exception