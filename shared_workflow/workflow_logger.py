import logging
import sys

from qcore.constants import ProcessType

logging.NOPRINTCRITICAL = logging.CRITICAL + 1
logging.NOPRINTERROR = logging.ERROR + 1
logging.NOPRINTWARNING = logging.WARNING + 1
logging.NOPRINTINFO = logging.INFO + 1
logging.NOPRINTDEBUG = logging.DEBUG + 1

logging.addLevelName(logging.NOPRINTCRITICAL, "NO_PRINT_CRITICAL")
logging.addLevelName(logging.NOPRINTERROR, "NO_PRINT_ERROR")
logging.addLevelName(logging.NOPRINTWARNING, "NO_PRINT_WARNING")
logging.addLevelName(logging.NOPRINTINFO, "NO_PRINT_INFO")
logging.addLevelName(logging.NOPRINTDEBUG, "NO_PRINT_DEBUG")

AUTO_SUBMIT_LOGGER_NAME = "auto_submit"

GENERAL_LOGGING_MESSAGE_FORMAT = (
    "%(levelname)8s -- %(asctime)s - %(module)s.%(filename)s.%(funcName)s - %(message)s"
)
general_formatter = logging.Formatter(GENERAL_LOGGING_MESSAGE_FORMAT)

TASK_LOGGING_MESSAGE_FORMAT = (
    "%(levelname)8s -- %(asctime)s - %(module)s.%(filename)s.%(funcName)s - "
    "{}.{} - %(message)s"
)


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
    logger.setLevel(logging.DEBUG)

    print_handler = logging.StreamHandler(sys.stdout)
    print_handler.setLevel(logging.INFO)
    print_handler.setFormatter(general_formatter)

    # If the message level ends in 1 do not print it to stdout
    print_handler.addFilter(lambda record: (record.levelno % 10) != 1)

    logger.addHandler(print_handler)

    return logger


def add_general_file_handler(logger: logging.Logger, file_path: str):
    """
    Adds a file handler to the logger using the given file_path
    :param logger: The logger object
    :param file_path: The path to the file to be used. Will be appended to if it already exists
    """
    file_out_handler = logging.FileHandler(file_path)
    file_out_handler.setFormatter(general_formatter)

    logger.addHandler(file_out_handler)


def get_task_logger(old_logger: logging.Logger, realisation: str, process_type: int):

    new_logger = logging.getLogger()
    new_logger.setLevel(logging.DEBUG)

    task_formatter = logging.Formatter(
        TASK_LOGGING_MESSAGE_FORMAT.format(
            realisation, ProcessType(process_type).str_value
        )
    )

    old_handlers = old_logger.handlers
    for handler in old_handlers:
        if isinstance(handler, logging.FileHandler):
            task_file_out_handler = logging.FileHandler(handler.baseFilename)
            task_file_out_handler.setFormatter(task_formatter)
            new_logger.addHandler(task_file_out_handler)

    task_print_handler = logging.StreamHandler(sys.stdout)
    task_print_handler.setLevel(logging.INFO)
    task_print_handler.setFormatter(task_formatter)

    # If the message level ends in 1 do not print it to stdout
    task_print_handler.addFilter(lambda record: (record.levelno % 10) != 1)

    new_logger.addHandler(task_print_handler)

    return new_logger
