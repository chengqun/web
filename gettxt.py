# -*- coding: UTF-8 -*-
import datetime
import requests # type: ignore
import json
import datetime
import sqlite3
import pandas as pd


# 连接到 SQLite 数据库（假设数据库文件为 'stock_data.db'）
conn = sqlite3.connect(r"../Lean/Data/AAshares/QuantConnectBase.db3") 
cursor = conn.cursor()


# 用于存储所有待插入的数据
all_values = []

# 假设 Excel 文件名为 'data.xlsx'，你可以根据实际情况修改
excel_file = pd.ExcelFile('2.xlsx')
# 假设数据在第一个工作表中，你可以根据实际情况修改表名
df = excel_file.parse(excel_file.sheet_names[0])
# 定义需要选取的列索引
columns_to_select = [0, 1, 2]
# 从 DataFrame 中选取前 4 列数据
# 复制原始 DataFrame 并选取指定列
selected_df = df.iloc[:, columns_to_select].copy()
# 将第一列（索引为 0）的数据转换为字符串类型
selected_df.iloc[:, 0] = selected_df.iloc[:, 0].astype(str)
# 将选取的数据转换为元组列表并添加到 all_values 列表中
data_tuples = [tuple(row) for row in selected_df.values]
# 处理参数 0 可能不支持的类型，假设第一列是日期类型，将其转换为字符串
# 处理日期格式，将类似 '2025-06-18 00:00:00' 的日期截取到 '2025-06-18'
processed_data_tuples = [(str(item[0]).split(' ')[0], item[1], item[2]) for item in data_tuples]
all_values.extend(processed_data_tuples)
# print(all_values)

insert_sql = '''
    INSERT OR IGNORE INTO LiveStockData ("Date","Code","StrategyName")
    VALUES (?,?,?)
'''
# 批量插入数据
cursor.executemany(insert_sql, all_values)
# 提交事务并关闭连接
conn.commit()
conn.close()

