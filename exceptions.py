class BaseBootstrapperException(Exception):
    pass


class BootstrapperInternalError(BaseBootstrapperException):
    pass


class BootstrapperInputError(BaseBootstrapperException):
    pass