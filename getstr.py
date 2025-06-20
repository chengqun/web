# -*- coding: UTF-8 -*-
import datetime
import requests # type: ignore
import json
import datetime
import sqlite3


# 连接到 SQLite 数据库（假设数据库文件为 'stock_data.db'）
conn = sqlite3.connect(r"../Lean/Data/AAshares/QuantConnectBase.db3") 
cursor = conn.cursor()

json_str = requests.get('http://ai.10jqka.com.cn/transfer/index/index?app=19').content
jsondata = json.loads(json_str)
data = jsondata['data']

# 用于存储所有待插入的数据
all_values = []

for x in data:
    strategy_data = x['stock_info']
    
    for stock_data in strategy_data:
        j = stock_data
        stockCode = j['stock_code']
        if stockCode.startswith("6"):
            processed_code= "SH." + stockCode
        elif stockCode.startswith("0") or stockCode.startswith("3"):
            processed_code= "SZ." + stockCode
        else:
            processed_code= ""
        values = (datetime.datetime.strptime(x['stockpicking_date'],'%Y%m%d').strftime('%Y-%m-%d'), processed_code, j['stock_name'], x['strategy_name'])
        print(values)
        all_values.append(values)

insert_sql = '''
    INSERT OR IGNORE INTO StrategyData ("Date", "Code", "Name", "StrategyName")
    VALUES (?,?,?,?)
'''
# 批量插入数据
cursor.executemany(insert_sql, all_values)

# 提交事务并关闭连接
conn.commit()
conn.close()

