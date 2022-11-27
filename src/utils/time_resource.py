import contextlib
from time import perf_counter
import logging

logger = logging.getLogger(__name__)

@contextlib.contextmanager
def clock(text):
    start = perf_counter()
    logger.debug("starting %s...", text)
    yield
    logger.info("finished %s in %.3f seconds", text, perf_counter() - start)
