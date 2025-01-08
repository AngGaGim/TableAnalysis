from core.nltable.agent.file_reading.data_normalizer import get_table_reformat_chain

from langchain_core.runnables import RunnableConfig
from langchain_openai import ChatOpenAI

from models.nltable import FileTableTypeEnum

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional
import os

from dotenv import load_dotenv

load_dotenv()

import logging
logger = logging.getLogger(__name__)

class TableAsyncService:

    @staticmethod
    def get_file_extension(filepath):
        from pathlib import Path
        ext = Path(f'{filepath}').suffix.lower().replace('.',"")
        return ext

    @staticmethod
    def preprocess(filepath):
        '''
        表格预处理，包含提取表头和数据清洗
        :return:
        '''


        ext = TableAsyncService.get_file_extension(filepath)

        if ext in [FileTableTypeEnum.XLS.value,FileTableTypeEnum.XLSX.value]:
            excel_file = pd.ExcelFile(filepath)
            table_sheets = excel_file.sheet_names
            print(f"文件中包含以下工作表：{table_sheets}")
            for sheet in table_sheets:
                origin_df = TableAsyncService._read_excel_to_df(excel_file,sheet)
                if not origin_df.empty:
                    # 提取表头
                    raw_table_info, reformatted_table = TableAsyncService.abstract_headers(origin_df)
                    # 更新表头字段
                    columns = reformatted_table[0]
                    print(f"工作表 '{sheet}' 的字段：{columns}")

        elif ext in FileTableTypeEnum.CSV.value:
            # 读取 CSV 文件
            df = pd.read_csv(filepath)
            columns = df.columns.tolist()  # 获取列名
            print(f"获取到的字段：{columns}")

        else:
            logger.error("不支持的文件格式")
            raise Exception("不支持的文件格式")


    @staticmethod
    def clean_df():
        '''
        数据清洗
        :return:
        '''


    @staticmethod
    def abstract_headers(origin_df: pd.DataFrame):
        '''
        提取表头
        :return:
        '''
        try:
            raw_table_info = origin_df.replace(np.nan, None).values.tolist()
            normalize_llm = ChatOpenAI(
                openai_api_base=os.getenv('NLTABLE_LLM_URL'),
                openai_api_key=os.getenv('NLTABLE_LLM_KEY'),
                model_name=os.getenv('NLTABLE_LLM_MODEL')
            )
            agent = get_table_reformat_chain(llm=normalize_llm)
            reformatted_table = agent.invoke(
                input={"table": raw_table_info},
                config=RunnableConfig(
                    max_concurrency=0,
                ),
            )
            print(f"raw_table_info: {raw_table_info}")
            print(f"reformatted_table: {reformatted_table}")

            return raw_table_info, reformatted_table

        except Exception as e:
            logger.error(f'{e.args}')
            raise Exception("获取表头失败")


    @staticmethod
    def _read_excel_to_df(origin_df,sheet_name) -> pd.DataFrame:
        '''
        读取指定sheet的原始文件，保留前10行数据，表头为空
        并对日期数据进行转换
        :param file_path:
        :param sheet_name:
        :return:
        '''
        origin_df = origin_df.parse(sheet_name, nrows=10, header=None)
        # origin_df = pd.read_excel(file_path, nrows=10, header=None, sheet_name=sheet_name)
        origin_df = origin_df.where(origin_df.notnull(), None).map(
            lambda cell: (
                cell.strftime('%Y-%m-%d') if isinstance(cell, (pd.Timestamp, datetime)) and pd.notnull(cell) else cell
            )
        )
        return origin_df


if __name__ == '__main__':
    filepath  = r"E:\表格解析\data\财务报表.xlsx"
    TableAsyncService.preprocess(filepath)