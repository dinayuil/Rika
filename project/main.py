import sqlite3
import akshare as ak
import pathlib
import os
import pandas as pd
import random
import time
from crawler import get_fund_basic_info
import re

"""
思路
1. 获得所有开放式基金代码
2. 通过基金代码获得基金基本信息
3. 建表，基本信息存入数据库
8. 获取基金历史净值
9. 建表，净值存入数据库
10. 基金业绩数据，回撤?
"""

funds_db_file_path = pathlib.Path(__file__).parent.parent / "data" / "funds.db"
print(funds_db_file_path)

def create_if_not_exists_db_tables():
    # 连接到SQLite数据库（如果不存在则创建）
    conn = sqlite3.connect(funds_db_file_path)
    cursor = conn.cursor()

    # 创建基金信息表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS funds (
        fund_id VARCHAR(20) PRIMARY KEY,  -- 基金代码
        fund_name TEXT,           -- 基金简称
        inception_date DATE,              -- 成立时间
        latest_scale DECIMAL(10, 2),      -- 最新规模（单位：亿）
        fund_type TEXT,            -- 基金类型
        trading_status TEXT,       -- 申购状态
        closed_period INTEGER,          -- 封闭时间
        estimated_opening_time TEXT,    -- 预估开放时间
        subscription_rate TEXT,         -- 申购费用
        redemption_period INTEGER,      -- 最低赎回费用所须持有日期
        redemption_rate TEXT,            -- 最低赎回费用
        fund_manager TEXT,                  -- 基金经理
        latest_manager_change_date DATE    -- 最新基金经理变动日期
    );
    ''')

    # 创建基金单位净值表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS fund_nav (
            fund_id VARCHAR(20),                -- 基金代码
            value_date DATE,                    -- 净值日期
            nav DECIMAL(10, 4),           -- 净值
            FOREIGN KEY (fund_id) REFERENCES funds(fund_id)
        )
    ''')

    # 创建基金累计净值表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS fund_cumulative_nav (
            fund_id VARCHAR(20),                -- 基金代码
            value_date DATE,                    -- 净值日期
            cumulative_nav DECIMAL(10, 4),      -- 累计净值
            FOREIGN KEY (fund_id) REFERENCES funds(fund_id)
        )
    ''')

    # 创建基金分红表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS fund_dividends (
            fund_id VARCHAR(20),                -- 基金代码
            ex_dividend_date DATE,              -- 除息日
            dividend_per_share DECIMAL(10, 4),  -- 每份分红
            FOREIGN KEY (fund_id) REFERENCES funds(fund_id)
        )
    ''')

    # 创建基金拆分表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS fund_splits (
            fund_id VARCHAR(20),                -- 基金代码
            split_date DATE,                    -- 拆分折算日
            split_ratio TEXT,                   -- 拆分折算比例
            FOREIGN KEY (fund_id) REFERENCES funds(fund_id)
        )
    ''')

    conn.commit()
    cursor.close()
    conn.close()

def save_all_fund_partial_data():
    # 连接数据库
    # create_if_not_exists_db()
    conn = sqlite3.connect(funds_db_file_path)
    cursor = conn.cursor()

    # 先用列表的部分信息一次性更新所有基金
    fund_open_fund_daily_em_df = ak.fund_open_fund_daily_em()
    partial_funds_basic_data = [tuple(row) for row in fund_open_fund_daily_em_df[['基金代码', '基金简称', '申购状态', '手续费']].values]
    
    insert_query = '''
    INSERT OR REPLACE INTO funds (
        fund_id,            fund_name,      
        trading_status,     subscription_rate)
    VALUES (?, ?, ?, ?);
    '''

    try:
        # 执行批量插入
        cursor.executemany(insert_query, partial_funds_basic_data)
        # 提交更改
        conn.commit()
        print("批量插入成功")
    except Exception as e:
        print(f"批量插入失败: {e}")
        conn.rollback()
    finally:
        # 关闭连接
        cursor.close()
        conn.close()


def get_all_fund_id_asc_from_db():
    """获取数据库funds表所有基金代码，升序排列

    Returns:
        list: funds表所有基金代码（升序排列）
    """
    conn = sqlite3.connect(funds_db_file_path)
    cursor = conn.cursor()

    cursor.execute('''
        SELECT fund_id
        FROM funds
        ORDER BY fund_id ASC;
    ''')
    fund_ids = [row[0] for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    return fund_ids


def save_all_fund_nav_data():
    conn = sqlite3.connect(funds_db_file_path)
    cursor = conn.cursor()

    fund_ids = get_all_fund_id_asc_from_db()

    # 获取每个基金的每日单位净值
    for id in fund_ids:
        print(id)
        fund_open_fund_info_em_df = ak.fund_open_fund_info_em(symbol=id, indicator="单位净值走势")
        fund_nav_data = [(id, row['净值日期'].isoformat(), row['单位净值']) for index, row in fund_open_fund_info_em_df.iterrows()]

        # 保存基金历史全部每日单位净值到数据库
        cursor.executemany('''
            INSERT INTO fund_nav (fund_id, value_date, nav)
            VALUES (?, ?, ?)
        ''', fund_nav_data)
        conn.commit()

    # conn.commit()
    cursor.close()
    conn.close()

create_if_not_exists_db_tables()
save_all_fund_nav_data()


# 待验证
def save_all_fund_cumulative_nav_data():
    conn = sqlite3.connect(funds_db_file_path)
    cursor = conn.cursor()

    fund_ids = get_all_fund_id_asc_from_db()

    for id in fund_ids:
        print(id)
        # 获取基金的每日累计净值
        fund_open_fund_info_em_df = ak.fund_open_fund_info_em(symbol=id, indicator="累计净值走势")
        fund_cumulative_nav_data = [(id, row['净值日期'].isoformat(), row['累计净值']) for index, row in fund_open_fund_info_em_df.iterrows()]

        # 保存基金历史全部每日单位净值到数据库
        cursor.executemany('''
            INSERT INTO fund_cumulative_nav (fund_id, value_date, cumulative_nav)
            VALUES (?, ?, ?)
        ''', fund_cumulative_nav_data)
        conn.commit()

    # conn.commit()
    cursor.close()
    conn.close()


# 待验证
def save_all_fund_dividend_data():
    conn = sqlite3.connect(funds_db_file_path)
    cursor = conn.cursor()

    fund_ids = get_all_fund_id_asc_from_db()
    for id in fund_ids:
        print(id)
        fund_open_fund_info_em_df = ak.fund_open_fund_info_em(symbol=id, indicator="分红送配详情")
        # print(fund_open_fund_info_em_df)
        fund_open_fund_info_em_df['每份分红'] = fund_open_fund_info_em_df['每份分红'].apply(lambda x: float(re.search(r'\d+\.\d+', x).group()) if re.search(r'\d+\.\d+', x) else None)
        fund_dividend_data = [(id, row['除息日'].isoformat(), row['每份分红']) for index, row in fund_open_fund_info_em_df.iterrows()]
        
        # 保存基金历史全部分红到数据库
        cursor.executemany('''
            INSERT INTO fund_nav (fund_id, ex_dividend_date, dividend_per_share)
            VALUES (?, ?, ?)
        ''', fund_dividend_data)
        conn.commit()

    cursor.close()
    conn.close()


# 待验证
def save_all_fund_split_data():
    conn = sqlite3.connect(funds_db_file_path)
    cursor = conn.cursor()

    # 提取拆分比例并计算比值
    def calculate_ratio(ratio_str):
        left, right = map(float, ratio_str.split(':'))
        return right / left

    fund_ids = get_all_fund_id_asc_from_db()
    for id in fund_ids:
        print(id)
        fund_open_fund_info_em_df = ak.fund_open_fund_info_em(symbol=id, indicator="拆分详情")
        # 拆分还有不同类型？可能需要进一步处理
        # fund_open_fund_info_em_df['拆分折算比例'] = fund_open_fund_info_em_df['拆分折算比例'].apply(calculate_ratio)
        # print(fund_open_fund_info_em_df)
        fund_dividend_data = [(id, row['拆分折算日'].isoformat(), row['拆分类型'], row['拆分折算比例']) for index, row in fund_open_fund_info_em_df.iterrows()]
        
        # 保存基金历史全部拆分到数据库
        cursor.executemany('''
            INSERT INTO fund_nav (fund_id, split_date, split_ratio)
            VALUES (?, ?, ?)
        ''', fund_dividend_data)
        conn.commit()

    cursor.close()
    conn.close()

# all_funds_basic_info = []
# i = 0
# 获取开放式基金列表
# fund_open_fund_daily_em_df = ak.fund_open_fund_daily_em()
# for row in fund_open_fund_daily_em_df.itertuples():
#     print(row.基金代码)
#     fund_basic_info = get_fund_basic_info(row.基金代码)
#     all_funds_basic_info.append(fund_basic_info)
#     # test
#     i += 1
#     if i == 9:
#         break

# data_to_insert = [
#     (
#         data['基金代码'],         data['基金简称'],        data['成立日'],        data['规模'],        data['类型'],
#         data['申购状态'],        data['封闭期'],        data['预估开放时间'],        data['申购费率'],        data['最低赎回费率适用期限'],
#         data['最低赎回费率'], data['基金经理'], data['最新经理变动日期']
#     )
#     for data in all_funds_basic_info
# ]

# insert_query = '''
# INSERT OR REPLACE INTO funds (
#     fund_id,            fund_name,      inception_date,         latest_scale,      fund_type,
#     trading_status,     closed_period,  estimated_opening_time, subscription_rate, redemption_period,
#     redemption_rate,    fund_manager,   latest_manager_change_date)
# VALUES (?, ?, ?, ?, ?,
#         ?, ?, ?, ?, ?,
#         ?, ?, ?);
# '''

# try:
#     # 执行批量插入
#     cursor.executemany(insert_query, data_to_insert)
#     # 提交更改
#     conn.commit()
#     print("批量插入成功")
# except Exception as e:
#     print(f"批量插入失败: {e}")
#     conn.rollback()
# finally:
#     # 关闭连接
#     cursor.close()
#     conn.close()



"""
查询基金基本信息，结果格式如下
      item                                              value
0     基金代码                                             000001
1     基金名称                                             华夏成长混合
2     基金全称                                            华夏成长前收费
3     成立时间                                         2001-12-18
4     最新规模                                             27.30亿
5     基金公司                                         华夏基金管理有限公司
6     基金经理                                            王泽实 万方方
7     托管银行                                       中国建设银行股份有限公司
8     基金类型                                             混合型-偏股
9     评级机构                                               晨星评级
10    基金评级                                               一星基金
11    投资策略  在股票投资方面，本基金重点投资于预期利润或收入具有良好增长潜力的成长型上市公司发行的股票，从...
12    投资目标  本基金属成长型基金，主要通过投资于具有良好成长性的上市公司的股票，在保持基金资产安全性和流动...
13  业绩比较基准                                       本基金暂不设业绩比较基准
"""
# fund_individual_basic_info_xq_df = ak.fund_individual_basic_info_xq(symbol="000001")
# fund_id = fund_individual_basic_info_xq_df.loc[fund_individual_basic_info_xq_df['item'] == '基金代码', 'value'].values[0]
# fund_name = fund_individual_basic_info_xq_df.loc[fund_individual_basic_info_xq_df['item'] == '基金名称', 'value'].values[0]
# inception_date = fund_individual_basic_info_xq_df.loc[fund_individual_basic_info_xq_df['item'] == '成立时间', 'value'].values[0]
# latest_scale = fund_individual_basic_info_xq_df.loc[fund_individual_basic_info_xq_df['item'] == '最新规模', 'value'].values[0].replace('亿', '')
# fund_type = fund_individual_basic_info_xq_df.loc[fund_individual_basic_info_xq_df['item'] == '基金类型', 'value'].values[0]

# print(fund_individual_basic_info_xq_df)
# print(fund_id, fund_name, inception_date, latest_scale, fund_type)


# 不使用下面方法的原因：基金列表是来自天天基金，但是申购状态来自雪球；天天基金的基金在雪球有可能找不到
# 获得所有开放式基金申购状态表
# funds_basic_infos = []

# fund_purchase_em_df = ak.fund_purchase_em()

# i = 0
# for row in fund_purchase_em_df.itertuples():
#     print(row.基金代码)
    
#     fund_individual_basic_info_xq_df = ak.fund_individual_basic_info_xq(symbol=row.基金代码)
#     fund_id = fund_individual_basic_info_xq_df.loc[fund_individual_basic_info_xq_df['item'] == '基金代码', 'value'].values[0]
#     fund_name = fund_individual_basic_info_xq_df.loc[fund_individual_basic_info_xq_df['item'] == '基金名称', 'value'].values[0]
#     inception_date = fund_individual_basic_info_xq_df.loc[fund_individual_basic_info_xq_df['item'] == '成立时间', 'value'].values[0]

#     latest_scale = fund_individual_basic_info_xq_df.loc[fund_individual_basic_info_xq_df['item'] == '最新规模', 'value'].values[0].replace('亿', '')
#     if '万' in latest_scale:
#         latest_scale = latest_scale.replace('万', '')
#     latest_scale = float(latest_scale)

#     fund_type = fund_individual_basic_info_xq_df.loc[fund_individual_basic_info_xq_df['item'] == '基金类型', 'value'].values[0]
#     subscription_status = row.申购状态
#     funds_basic_infos.append((fund_id, fund_name, inception_date, latest_scale, fund_type, subscription_status))
#     delay = random.uniform(0.5, 2.0) # 随机生成0.5~2.0秒的间隔
#     time.sleep(delay)
#     i += 1
#     if i == 10:
#         break;



"""
查询交易费用，结果格式如下：
   费用类型                条件或名称       费用
0  买入规则          买入金额<100.0万     0.80
1  买入规则  100.0万<=买入金额<300.0万     0.50
2  买入规则  300.0万<=买入金额<500.0万     0.30
3  买入规则         500.0万<=买入金额  1000.00
4  卖出规则       0.0天<持有期限<7.0天     1.50
5  卖出规则    7.0天<=持有期限<365.0天     0.10
6  卖出规则    365.0天<=持有期限<2.0年     0.05
7  卖出规则           2.0年<=持有期限     0.00
8  其他费用                基金管理费     0.30
9  其他费用                基金托管费     0.10
"""
# fund_individual_detail_info_xq_df = ak.fund_individual_detail_info_xq(symbol="002549")
# print(fund_individual_detail_info_xq_df)


# fund_open_fund_info_em_df = ak.fund_open_fund_info_em(symbol="000001", indicator="单位净值走势")
# print(fund_open_fund_info_em_df)
