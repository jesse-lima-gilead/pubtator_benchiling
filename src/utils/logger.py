import logging


class SingletonLogger:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls, *args, **kwargs)
            cls._instance.logger = logging.getLogger(__name__)
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s - %(levelname)s - %(filename)s - %(message)s",
            )
        return cls._instance

    def get_logger(self):
        return self.logger
