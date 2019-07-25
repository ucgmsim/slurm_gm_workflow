import logging
import sys

from qcore.constants import ProcessType
from qcore import qclogging

TASK_LOGGING_MESSAGE_FORMAT = (
    "%(levelname)8s -- %(asctime)s - %(module)s.%(funcName)s - {}.{} - %(message)s"
)
TASK_THREADED_LOGGING_MESSAGE_FORMAT = "%(levelname)8s -- %(asctime)s - %(threadName)s - %(module)s.%(funcName)s - {}.{} - %(message)s"

REALISATION_LOGGING_MESSAGE_FORMAT = (
    "%(levelname)8s -- %(asctime)s - %(module)s.%(funcName)s - {} - %(message)s"
)
REALISATION_THREADED_LOGGING_MESSAGE_FORMAT = "%(levelname)8s -- %(asctime)s - %(threadName)s - %(module)s.%(funcName)s - {} - %(message)s"


def get_realisation_logger(
    old_logger: logging.Logger, realisation: str
) -> logging.Logger:
    """Creates a new logger that logs the realisation.
    The logger passed in is effectively duplicated and log messages are saved to the same file as the original logger.
    :param old_logger: Logger the new instance is to be based on
    :param realisation: The name of the realisation this logger is for
    :param threaded: If the logger is operating in a thread then record the name of the thread
    :return: The new logger object
    """
    new_logger = logging.getLogger(realisation)
    new_logger.setLevel(logging.DEBUG)

    if old_logger.name.startswith(qclogging.THREADED):
        task_formatter = logging.Formatter(
            REALISATION_THREADED_LOGGING_MESSAGE_FORMAT.format(realisation)
        )
    else:
        task_formatter = logging.Formatter(
            REALISATION_LOGGING_MESSAGE_FORMAT.format(realisation)
        )

    old_handlers = old_logger.handlers
    log_files = []
    for handler in old_handlers:
        if isinstance(handler, logging.FileHandler):
            log_name = handler.baseFilename
            if log_name in log_files:
                continue
            log_files.append(log_name)
            task_file_out_handler = logging.FileHandler(log_name)
            task_file_out_handler.setFormatter(task_formatter)
            new_logger.addHandler(task_file_out_handler)

    task_print_handler = logging.StreamHandler(sys.stdout)
    task_print_handler.setLevel(logging.INFO)
    if old_logger.name.startswith(qclogging.THREADED):
        task_print_handler.setFormatter(qclogging.stdout_threaded_formatter)
    else:
        task_print_handler.setFormatter(qclogging.stdout_formatter)

    # If the message level ends in 1 do not print it to stdout
    task_print_handler.addFilter(lambda record: (record.levelno % 10) != 1)

    new_logger.addHandler(task_print_handler)

    return new_logger


def get_task_logger(
    old_logger: logging.Logger,
    realisation: str,
    process_type: int,
) -> logging.Logger:
    """Creates a new logger that logs the realisation and process type.
    The logger passed in is effectively duplicated and log messages are saved to the same file as the original logger.
    :param old_logger: Logger the new instance is to be based on
    :param realisation: The name of the realisation this logger is for
    :param process_type: The type of process these logs are generated from
    :return: The new logger object
    """

    process_name = ProcessType(process_type).str_value

    new_logger = logging.getLogger("{}.{}".format(realisation, process_name))
    new_logger.setLevel(logging.DEBUG)

    if old_logger.name.startswith(qclogging.THREADED):
        task_formatter = logging.Formatter(
            TASK_THREADED_LOGGING_MESSAGE_FORMAT.format(realisation, process_name)
        )
    else:
        task_formatter = logging.Formatter(
            TASK_LOGGING_MESSAGE_FORMAT.format(realisation, process_name)
        )

    old_handlers = old_logger.handlers
    log_files = []
    for handler in old_handlers:
        if isinstance(handler, logging.FileHandler):
            log_name = handler.baseFilename
            if log_name in log_files:
                continue
            log_files.append(log_name)
            task_file_out_handler = logging.FileHandler(log_name)
            task_file_out_handler.setFormatter(task_formatter)
            new_logger.addHandler(task_file_out_handler)

    task_print_handler = logging.StreamHandler(sys.stdout)
    task_print_handler.setLevel(logging.INFO)
    if old_logger.name.startswith(qclogging.THREADED):
        task_print_handler.setFormatter(qclogging.stdout_threaded_formatter)
    else:
        task_print_handler.setFormatter(qclogging.stdout_formatter)

    # If the message level ends in 1 do not print it to stdout
    task_print_handler.addFilter(lambda record: (record.levelno % 10) != 1)

    new_logger.addHandler(task_print_handler)

    return new_logger


