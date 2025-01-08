import pandas as pd
from datetime import datetime


class TableAsyncService:

    @staticmethod
    def preprocess():
        '''
        表格预处理，包含提取表头和数据清洗
        :return:
        '''
        pass

    @staticmethod
    def clean_df():
        '''
        数据清洗
        :return:
        '''

    @staticmethod
    def abstract_headers():
        '''
        提取表头
        :return:
        '''
        pass

    @staticmethod
    def _read_excel_to_df(file_path,sheet_name) -> pd.DataFrame:
        '''
        读取指定sheet的原始文件，保留前10行数据，表头为空
        并对日期数据进行转换
        :param file_path:
        :param sheet_name:
        :return:
        '''
        origin_df = pd.read_excel(file_path, nrows=10, header=None, sheet_name=sheet_name)
        origin_df = origin_df.where(origin_df.notnull(), None).map(
            lambda cell: (
                cell.strftime('%Y-%m-%d') if isinstance(cell, (pd.Timestamp, datetime)) and pd.notnull(cell) else cell
            )
        )
        return origin_df
