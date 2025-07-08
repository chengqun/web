import time
import easyquotation
import pandas as pd
import json
from datetime import datetime, date, timedelta
from datetime import time as dt_time
import os

# 创建保存数据的目录
DATA_DIR = "stock_data"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

last_stocks = None  # 用于保存上一次请求的数据

def save_to_json(data, filename):
    """将数据保存为JSON文件"""
    filepath = os.path.join(DATA_DIR, filename)
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"数据已保存到 {filepath}")
    except Exception as e:
        print(f"保存JSON文件失败: {str(e)}")

def getall(date1_str):
    global last_stocks  # 声明使用全局变量
    # 初始化腾讯接口
    quotation = easyquotation.use('tencent')  
    all_data = quotation.market_snapshot(prefix=True)  
    stock_list = [
        {
            "code": f"{code[:2]}.{code[2:]}",      # 代码格式 sz000001 -> sz.000001
            "open": data.get("open", 0.0),          # 开盘价（当日）
            "close": data.get("now", 0.0),         # 当前价
            "high": data.get("high", 0.0),         # 当日最高价
            "low": data.get("low", 0.0),           # 当日最低价
            "volume": data.get("volume", 0),       # 成交量（股数）
            "amount": data.get("成交额(万)", 0.0)  # 成交额元转万元
        }
        for code, data in all_data.items()
    ]
    
    result_data = {
        "timestamp": date1_str,
        "stocks": []
    }
    
    # 计算成交量变化（需保存前次数据）
    if last_stocks:
        for stock in stock_list:
            prev = next((s for s in last_stocks if s["code"] == stock["code"]), None)
            if prev:
                try:
                    volume = stock["volume"] - prev["volume"]
                    amount = stock["amount"] - prev["amount"]
                except ValueError:
                    volume, amount = 0.0, 0.0
                
                result_data["stocks"].append({
                    "code": stock["code"],
                    "open": prev["close"],  # 使用前一次的收盘价作为开盘价
                    "close": stock["close"],
                    "high": stock["high"],
                    "low": stock["low"],
                    "volume": volume,
                    "amount": amount
                })
        
        # 生成JSON文件名
        filename = f"stock_5min_{date1_str.replace(':', '').replace(' ', '_')}.json"
        save_to_json(result_data, filename)
    
    last_stocks = stock_list.copy()

print("程序启动...")
while True:
    current_time = datetime.now()
    # 计算最近的整5分钟（四舍五入）
    rounded_minute = (current_time.minute + 2) // 5 * 5
    if rounded_minute >= 60:
        date1 = (current_time.replace(minute=0, second=0, microsecond=0) 
                + timedelta(hours=1))
    else:
        date1 = current_time.replace(minute=rounded_minute, second=0, microsecond=0)

    date1_str = date1.strftime('%Y-%m-%d %H:%M:%S')
    
    # 只在A股交易时间运行
    if ((dt_time(9, 30) <= datetime.now().time() <= dt_time(11, 31)) or \
       (dt_time(13, 0) < datetime.now().time() <= dt_time(15, 0)):
        getall(date1_str)
        time.sleep(300)  # 5分钟请求一次
    
    if datetime.now().time() >= dt_time(15, 0):  # 收盘后退出
        break