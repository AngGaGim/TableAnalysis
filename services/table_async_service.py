import pandas as pd



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
        origin_df = pd.read_excel(file_path,nrows=10,header=None,sheet_name=sheet_name)
