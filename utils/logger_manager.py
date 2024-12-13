import os
import time
import logging
from logging.handlers import QueueHandler, QueueListener
from queue import Queue


class LoggerManager:

    def __init__(self, script_name: str, log_dir: str = "logs"):

        self.script_name = script_name
        self.log_dir = log_dir
        self.log_queue = Queue()
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:

        sub_dir_name = os.path.splitext(os.path.basename(self.script_name))[0]
        sub_log_dir = os.path.join(self.log_dir, sub_dir_name)
        os.makedirs(sub_log_dir, exist_ok=True)

        log_file_name = f"{sub_dir_name}_{time.strftime('%Y%m%d_%H%M%S')}.log"
        log_file_path = os.path.join(sub_log_dir, log_file_name)

        logger = logging.getLogger(sub_dir_name)
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            # Configurar el manejador de cola
            queue_handler = QueueHandler(self.log_queue)
            logger.addHandler(queue_handler)

            # Configurar el manejador de archivo
            file_handler = logging.FileHandler(log_file_path)

            # Agregar información de archivo y línea al formato del log
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(pathname)s - Line %(lineno)d - %(message)s'
            )
            file_handler.setFormatter(formatter)

            # Iniciar el listener de cola
            listener = QueueListener(self.log_queue, file_handler)
            listener.start()

        return logger

    def get_logger(self) -> logging.Logger:

        return self.logger
