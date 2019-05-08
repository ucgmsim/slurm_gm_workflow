import logging
import sys

logging.NOPRINTCRITICAL = logging.CRITICAL+1
logging.NOPRINTERROR = logging.ERROR+1
logging.NOPRINTWARNING = logging.WARNING+1
logging.NOPRINTINFO = logging.INFO+1
logging.NOPRINTDEBUG = logging.DEBUG+1

logging.addLevelName(logging.NOPRINTCRITICAL, "NO_PRINT_CRITICAL")
logging.addLevelName(logging.NOPRINTERROR, "NO_PRINT_ERROR")
logging.addLevelName(logging.NOPRINTWARNING, "NO_PRINT_WARNING")
logging.addLevelName(logging.NOPRINTINFO, "NO_PRINT_INFO")
logging.addLevelName(logging.NOPRINTDEBUG, "NO_PRINT_DEBUG")


AUTO_SUBMIT_LOGGER_NAME = "auto_submit"
LOGGING_MESSAGE_FORMAT = "%(levelname)8s -- %(asctime)s - %(filename)s.%(funcName)s - %(message)s"
formatter = logging.Formatter(LOGGING_MESSAGE_FORMAT)


def log(logger, level, message):
    """
    If the logger is not None then the message is logged with the given level.
    Otherwise the message is printed to stdout for info messages, or stderr for higher levels.
    If print is used the NOPRINT options are obsolete and the message is printed anyway.
    :param logger: logger object to be used to log
    :param level: The level of the message
    :param message: The message to be logged
    """
    if logger is not None:
        logger.log(level, message)
    else:
        if level > logging.NOPRINTINFO:
            print(message, file=sys.stderr)
        elif level > logging.NOPRINTDEBUG:
            print(message)


def get_logger():
    """
    Creates a logger and an associated handler to print messages over level INFO to stdout.
    The handler is configured such that messages will not be printed if their underlying level value ends in 1, this is
    mostly used for logging fatal exceptions that will be printed to stdout/stderr anyway
    :return: The logger object
    """
    logger = logging.getLogger(AUTO_SUBMIT_LOGGER_NAME)

    print_handler = logging.StreamHandler(sys.stdout)
    print_handler.setLevel(logging.INFO)
    print_handler.setFormatter(formatter)

    # If the message level ends in 1 do not print it to stdout
    print_handler.addFilter(lambda record: (record.levelno % 10) != 1)

    logger.addHandler(print_handler)

    return logger


def add_file_handler(logger, file_path):
    """
    Adds a file handler to the logger using the given file_path
    :param logger: The logger object
    :param file_path: The path to the file to be used. Will be appended to if it already exists
    """
    file_out_handler = logging.FileHandler(file_path)
    file_out_handler.setFormatter(formatter)

    logger.addHandler(file_out_handler)


