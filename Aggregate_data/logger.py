import logging
from logging.handlers import TimedRotatingFileHandler


class Logger:
    def __init__(self, module):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s %(levelname)s %(module)s [%(lineno)d] - %(funcName)s: %(message)s',
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        self.logger = logging.getLogger()
        self.logName = module.lower().replace(' ', '_')
        self.handler = TimedRotatingFileHandler(
            "logs/" + self.logName + ".log",
            when="midnight",
            interval=1,
            backupCount=30,
            encoding="utf-8"
        )
        self.logFormatter = logging.Formatter(
            '%(asctime)s.%(msecs)03d %(levelname)s %(module)s [%(lineno)d] - %(funcName)s: %(message)s'
        )
        self.handler.setFormatter(self.logFormatter)
        self.logger.addHandler(self.handler)
        print('Logger created.')

    def getLogger(self):
        return self.logger

    def __del__(self):
        print('Logger deleted.')
