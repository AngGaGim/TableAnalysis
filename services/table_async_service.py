import pandas as pd
import numpy as np
import os
from datetime import datetime
from models.nltable import FileTableTypeEnum
from typing import List, Tuple


class TableAsyncService:

    @staticmethod
    def preprocess(filepath: str):
        '''
        表格预处理，包含提取表头和数据清洗
        :param filepath: 文件路径
        :return:
        '''
        try:
            # 获取文件扩展名
            ext = TableAsyncService.get_file_extension(filepath)

            if ext in [FileTableTypeEnum.XLS.value, FileTableTypeEnum.XLSX.value]:
                # 处理 Excel 文件
                TableAsyncService._process_excel_file(filepath)

            elif ext in FileTableTypeEnum.CSV.value:
                # 处理 CSV 文件
                TableAsyncService._process_csv_file(filepath)

            else:
                raise ValueError("不支持的文件格式")

        except Exception as e:
            print(f"预处理失败: {str(e)}")
            raise

    @staticmethod
    def _process_excel_file(filepath: str):
        '''
        处理 Excel 文件，提取表头及相关数据
        :param filepath: 文件路径
        :return:
        '''
        try:
            # 加载 Excel 文件
            excel_file = pd.ExcelFile(filepath)
            table_sheets = excel_file.sheet_names
            print(f"文件中包含以下工作表：{table_sheets}")

            for sheet in table_sheets:
                # 读取每个工作表的前10行数据
                origin_df = TableAsyncService._read_excel_to_df(filepath, sheet)
                if not origin_df.empty:
                    # 提取表头
                    raw_table_info, reformatted_table = TableAsyncService.abstract_headers(origin_df)
                    # 更新表头字段
                    columns = reformatted_table[0] if reformatted_table else []
                    print(f"工作表 '{sheet}' 的字段：{columns}")

        except Exception as e:
            print(f"处理 Excel 文件失败: {str(e)}")
            raise

    @staticmethod
    def _process_csv_file(filepath: str):
        '''
        处理 CSV 文件，提取表头及相关数据
        :param filepath: 文件路径
        :return:
        '''
        try:
            # 读取 CSV 文件
            df = pd.read_csv(filepath)
            columns = df.columns.tolist()  # 获取列名
            print(f"获取到的字段：{columns}")

        except Exception as e:
            print(f"处理 CSV 文件失败: {str(e)}")
            raise

    @staticmethod
    def clean_df(df: pd.DataFrame) -> pd.DataFrame:
        '''
        数据清洗
        :param df: 输入的 DataFrame
        :return: 清洗后的 DataFrame
        '''
        try:
            # 清洗逻辑，这里根据实际需求添加清洗步骤
            df_cleaned = df.dropna()  # 示例：去掉所有含有NaN的行
            print("数据清洗完成")
            return df_cleaned

        except Exception as e:
            print(f"数据清洗失败: {str(e)}")
            raise

    @staticmethod
    def abstract_headers(origin_df: pd.DataFrame) -> Tuple[List[List], List[List]]:
        '''
        提取表头
        :param origin_df: 原始 DataFrame
        :return: 返回表头信息和格式化后的表格
        '''
        try:
            raw_table_info = origin_df.replace(np.nan, None).values.tolist()

            # 假设有一个处理 LLM 的链条
            normalize_llm = ChatOpenAI(
                openai_api_base=os.getenv('NLTABLE_LLM_URL'),
                openai_api_key=os.getenv('NLTABLE_LLM_KEY'),
                model_name=os.getenv('NLTABLE_LLM_MODEL')
            )
            agent = get_table_reformat_chain(llm=normalize_llm)
            reformatted_table = agent.invoke(
                input={"table": raw_table_info},
                config=RunnableConfig(max_concurrency=0),
            )

            print(f"raw_table_info: {raw_table_info}")
            print(f"reformatted_table: {reformatted_table}")

            return raw_table_info, reformatted_table

        except Exception as e:
            print(f"获取表头失败: {str(e)}")
            raise

    @staticmethod
    def _read_excel_to_df(file_path: str, sheet_name: str) -> pd.DataFrame:
        '''
        读取指定sheet的原始文件，保留前10行数据，表头为空
        并对日期数据进行转换
        :param file_path: 文件路径
        :param sheet_name: 工作表名称
        :return: 返回处理后的 DataFrame
        '''
        try:
            # 读取前10行数据，不带表头
            origin_df = pd.read_excel(file_path, nrows=10, header=None, sheet_name=sheet_name)

            # 处理日期格式
            origin_df = origin_df.applymap(
                lambda cell: cell.strftime('%Y-%m-%d') if isinstance(cell, (pd.Timestamp, datetime)) and pd.notnull(
                    cell) else cell
            )

            # 删除所有值为 NaN 的列（即空列）
            origin_df = origin_df.dropna(axis=1, how='all')

            # 将 NaN 转为 None
            origin_df = origin_df.where(origin_df.notnull(), None)


            print(f"成功读取工作表: {sheet_name}，数据预处理完成")

            return origin_df

        except Exception as e:
            print(f"读取 Excel 工作表 '{sheet_name}' 失败: {str(e)}")
            raise

    @staticmethod
    def get_file_extension(filepath: str) -> str:
        '''
        获取文件扩展名
        :param filepath: 文件路径
        :return: 文件扩展名
        '''
        return os.path.splitext(filepath)[-1].lower()
