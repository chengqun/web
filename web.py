import csv
from io import StringIO
from flask import Flask, jsonify, request, Response
import sqlite3
from datetime import datetime, timedelta, timezone
import pandas as pd
import requests

app = Flask(__name__)

def getdatahttp(code,table_name):
    # 请求第三方接口

    if code.startswith("SH.") or code.startswith("sh."):
        secid = "1." + code[3:]
    elif code.startswith("SZ.") or code.startswith("sz."):
        secid = "0." + code[3:]
    else:
        secid = code  # 保持原样，或根据需要处理其他情况
    # end_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    end_date = (datetime.now()).strftime('%Y%m%d')
    if(table_name=="stock_5min_k"):
        api_url = f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61&klt=5&fqt=1&end={end_date}&lmt=1488"  # 替换为实际API地址
    else:
        api_url = f"https://push2his.eastmoney.com/api/qt/stock/kline/get?secid={secid}&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61&klt=101&fqt=1&end={end_date}&lmt=1488"  # 替换为实际API地址

    # 模拟浏览器请求，添加请求头
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    try:
        response = requests.get(api_url, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        return {"error": f"API请求失败: {str(e)}"}, 500

    # 解析原始数据
    raw_data = response.json()
    klines = raw_data.get('data', {}).get('klines', [])
    
    # 数据清洗转换
    converted_data = []
    for item in klines:
        parts = item.split(',')
        if len(parts) < 11:
            continue  # 跳过无效数据
            
        # 按字段位置解析数据
        # 按字段位置解析数据
        if table_name == "stock_5min_k":
            # 处理带时间的5分钟K线数据
            local_time = datetime.strptime(parts[0] + ":00", "%Y-%m-%d %H:%M:%S")  # 添加秒并解析为本地时间
        else:
            # 处理其他表的日期（加一天逻辑）
            date_obj = datetime.strptime(parts[0], "%Y-%m-%d").date()  # 转换为date对象[1,4](@ref)
            new_date = date_obj + timedelta(days=1)  # 核心加一天操作[1,5,6](@ref)
            local_time = datetime.combine(new_date, datetime.min.time())  # 转换为带时间的datetime对象 
        # if(table_name=="stock_5min_k"):
        #     local_time = datetime.strptime(parts[0] + ":00", "%Y-%m-%d %H:%M:%S")  # 添加秒并解析为本地时间
        # else:
        #     local_time = datetime.strptime(parts[0] , "%Y-%m-%d")  # 添加秒并解析为本地时间
        date_time_str = local_time.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")  # 转换为UTC时间
        converted = {
            "Date": date_time_str,
            "Open": float(parts[1]),
            "Close": float(parts[2]),
            "High": float(parts[3]),
            "Low": float(parts[4]),
            "Volume": float(parts[5]),  # 交易量
            "Amount": float(parts[6]),    # 交易额
            "StrategyName": "",  # 修正字段索引[2,6](@ref)
            "NextOpen":0,
            "NextClose":0,
            "Next2Open":0,
            "Next2Close":0,
            "Next5Close":0
        }
        converted_data.append(converted)
    return converted_data

def handle_request(code, table_name, format_type='json'):
    converted_data = getdatahttp(code,table_name)
    # 返回数据
    if not converted_data:
        return {"error": "没有有效数据"}, 404
    # 从sqlite中读取数据，将converted_data 列表中StrategyName字段补全，根据日期匹配
    # 连接到 SQLite 数据库（假设数据库文件为 'stock_data.db'）
    conn = sqlite3.connect(r"../Lean/Data/AAshares/QuantConnectBase.db3") 
    cursor = conn.cursor()

    # 执行 SQL 查询，从 LiveStockData 表中获取日期和策略名称，根据 code 筛选
    query = "SELECT date, StrategyName FROM LiveStockData WHERE code = ?"
    cursor.execute(query, (code,))

    # 获取查询结果
    sqlite_data = cursor.fetchall()

    # 将查询结果转换为字典，键为日期，值为策略名称
    sqlite_dict = {row[0]: row[1] for row in sqlite_data}

    # 遍历 converted_data 列表，根据日期匹配补全 StrategyName 字段
    for item in converted_data:
        date_str = item["Date"].split(' ')[0]  # 提取日期部分
        if date_str in sqlite_dict:
            item["StrategyName"] = sqlite_dict[date_str]

    # 关闭连接
    conn.close()
        

    # converted_data是一个时序数组
    # 处理数据NextOpen为下一条的Open，Next2Opne为下下条的Open
    # 处理数据NextClose为下一条的Close，Next2Close为下下条的Close
    # 处理数据StrategyName为下一条的StrategyName，Next2StrategyName为下下条的StrategyName
    if(table_name=="stock_day_k"):
        for i in range(len(converted_data)):
            if i < len(converted_data) - 1:
                converted_data[i]["NextOpen"] = converted_data[i + 1]["Open"]
                converted_data[i]["NextClose"] = converted_data[i + 1]["Close"]
            if i < len(converted_data) - 2:
                converted_data[i]["Next2Open"] = converted_data[i + 2]["Open"]
                converted_data[i]["Next2Close"] = converted_data[i + 2]["Close"]
            if i < len(converted_data) - 5:
                # 计算5天后的收益，收益计算公式为：(5天后收盘价 - 当前收盘价) / 当前收盘价
                converted_data[i]["Next5Close"] = converted_data[i + 5]["Close"]

    now = datetime.now()
    start_time = now.replace(hour=9, minute=15, second=0, microsecond=0)
    end_time = now.replace(hour=15, minute=0, second=0, microsecond=0)
    if start_time <= now <= end_time:
        converted_data = converted_data[:-1]

    

    # 根据要求格式化输出
    if format_type == 'csv':
        # 生成CSV
        si = StringIO()
        writer = csv.DictWriter(si, fieldnames=converted_data[0].keys())
        writer.writeheader()
        writer.writerows(converted_data)  # 写入多行数据
        return si.getvalue(), 200, {'Content-Type': 'text/csv'}
    return jsonify(converted_data[-1]), 200

@app.route('/api/dayapi/<code>', methods=['GET'])
def get_daystock_data(code):
    format_type = request.args.get('format', default='json')
    return handle_request(code, 'stock_day_k', format_type)

@app.route('/api/dayapi/csv/<code>', methods=['GET'])
def get_daystock_data_csv(code):
    # 强制返回csv格式
    return handle_request(code, 'stock_day_k', 'csv')

@app.route('/api/minapi/<code>', methods=['GET'])
def get_stock_data(code):
    format_type = request.args.get('format', default='json')
    return handle_request(code, 'stock_5min_k', format_type)

@app.route('/api/minapi/csv/<code>', methods=['GET'])
def get_stock_data_csv(code):
    # 强制返回csv格式
    return handle_request(code, 'stock_5min_k', 'csv')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True)