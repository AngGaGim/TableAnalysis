import os
import logging
from logging.handlers import TimedRotatingFileHandler
from extensions.ext_storage import storage


class TableLogger:
    def __init__(self):
        # 获取日志文件存储路径
        log_path = storage.get_full_path('nltable')

        # 创建日志文件夹（如果不存在）
        os.makedirs(log_path, exist_ok=True)

        # 创建 logger 实例
        self.logger = logging.getLogger('nltable')
        self.logger.setLevel(logging.DEBUG)  # 设置日志级别

        # 创建并配置 TimedRotatingFileHandler
        log_filename = os.path.join(log_path, 'output')
        self.handler = TimedRotatingFileHandler(log_filename, when="midnight", interval=1)
        self.handler.suffix = "_%Y_%m_%d.log"  # 设置日志文件的后缀格式

        # 设置日志输出格式
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(funcName)s %(threadName)s %(lineno)d %(message)s')
        self.handler.setFormatter(formatter)
        self.logger.addHandler(self.handler)


# 创建 TableLogger 实例
table_logger = TableLogger()
