import logging.handlers
import logging


formatter = logging.Formatter(
    fmt='(%(levelname)-8s) %(asctime)s - %(name)s | %(message)s'
)

logger_file_handler_all = logging.handlers.WatchedFileHandler(
    "/home/bigdata/logs/bigdataproject/logs.log"
)
logger_file_handler_all.setLevel(logging.INFO)
logger_file_handler_all.setFormatter(formatter)

logger_file_handler_error = logging.handlers.WatchedFileHandler(
    "/home/bigdata/logs/bigdataproject/error.log"
)
logger_file_handler_error.setLevel(logging.ERROR)
logger_file_handler_error.setFormatter(formatter)

logger_console_handler = logging.StreamHandler()
logger_console_handler.setLevel(logging.WARNING)
logger_console_handler.setFormatter(formatter)

def config_logger(logger: logging.Logger):
    logger.addHandler(logger_console_handler)
    logger.addHandler(logger_file_handler_all)
    logger.addHandler(logger_file_handler_error)
    return logger
