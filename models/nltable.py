from enum import Enum


class FileTableTypeEnum(Enum):
    """
    表格文件类型
    """
    XLSX = 'xlsx'
    XLS = 'xls'
    CSV = 'csv'