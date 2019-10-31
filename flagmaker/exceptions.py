class BaseFlagMakerException(Exception):
    pass


class FlagMarkerInternalError(BaseFlagMakerException):
    pass


class FlagMakerInputError(BaseFlagMakerException):
    pass


class FlagMakerConfigurationError(BaseFlagMakerException):
    pass