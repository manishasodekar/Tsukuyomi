import logging
import traceback
from sentry_sdk import push_scope, capture_exception
import heconstants


class SentryUtilFunctions:
    logger = logging.getLogger(heconstants.EXECUTOR_LOGGER_NAME)

    def __init__(self):
        pass

    def send_event(self, e: Exception, trace, level: str = "debug"):
        try:
            with push_scope() as scope:
                scope.set_tag("isValid", True)
                scope.set_level(level)
                scope.set_extra("traceback", trace)
                capture_exception(e)
        except Exception as exc:
            msg = "Failed in send_event()"
            trace = traceback.format_exc()
            SentryUtilFunctions().send_event(exc, trace)
