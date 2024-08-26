import sqlite3
import pathlib
import akshare as ak
import re

default_db_file_path = pathlib.Path(__file__).parent.parent / "data" / "funds.db"

def create_if_not_exists_db_tables(funds_db_file_path):
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
            split_type TEXT,                    -- 拆分类型
            split_ratio TEXT,                   -- 拆分折算比例
            FOREIGN KEY (fund_id) REFERENCES funds(fund_id)
        )
    ''')

    conn.commit()
    cursor.close()
    conn.close()


def save_all_fund_partial_data(funds_db_file_path: str):
    """获取所有开放式基金的基金代码、基金简称、申购状态、手续费，存入数据库funds表

    Args:
        funds_db_file_path (str): 数据库文件路径
    """
    # 连接数据库
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


def get_all_fund_id_asc_from_db(funds_db_file_path: str):
    """获取数据库funds表所有基金代码，升序排列

    Args:
        funds_db_file_path (str): 数据库文件路径

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


def get_fund_all_nav_data(fund_id: str):
    fund_nav_data = []
    fund_open_fund_info_em_df = ak.fund_open_fund_info_em(symbol=fund_id, indicator="单位净值走势")
    if fund_open_fund_info_em_df is not None:
        fund_nav_data = [(fund_id, row['净值日期'].isoformat(), row['单位净值']) for index, row in fund_open_fund_info_em_df.iterrows()]
    return fund_nav_data


def save_all_fund_nav_data(funds_db_file_path: str):
    """获取funds表里所有基金的每日单位净值，存入fund_nav表。耗时很长。

    Args:
        funds_db_file_path (str): _description_
    """
    conn = sqlite3.connect(funds_db_file_path)
    cursor = conn.cursor()

    fund_ids = get_all_fund_id_asc_from_db()

    # 获取每个基金的每日单位净值
    for fund_id in fund_ids:
        print(fund_id)
        fund_nav_data = get_fund_all_nav_data(fund_id)

        # 保存基金历史全部每日单位净值到数据库
        cursor.executemany('''
            INSERT INTO fund_nav (fund_id, value_date, nav)
            VALUES (?, ?, ?)
        ''', fund_nav_data)
        conn.commit()

    # conn.commit()
    cursor.close()
    conn.close()


def get_fund_all_split_data_for_save(fund_id: str):
    fund_dividend_data = []
    fund_open_fund_info_em_df = ak.fund_open_fund_info_em(symbol=fund_id, indicator="拆分详情")
    # 拆分还有不同类型？可能需要进一步处理
    # fund_open_fund_info_em_df['拆分折算比例'] = fund_open_fund_info_em_df['拆分折算比例'].apply(calculate_ratio)
    # print(fund_open_fund_info_em_df)
    if fund_open_fund_info_em_df is not None:
        fund_dividend_data = [(fund_id, row['拆分折算日'], row['拆分类型'], row['拆分折算比例']) for index, row in fund_open_fund_info_em_df.iterrows()]
    return fund_dividend_data


# 待验证
def save_all_fund_split_data(funds_db_file_path: str):
    conn = sqlite3.connect(funds_db_file_path)
    cursor = conn.cursor()

    # 提取拆分比例并计算比值
    def calculate_ratio(ratio_str):
        left, right = map(float, ratio_str.split(':'))
        return right / left

    fund_ids = get_all_fund_id_asc_from_db()
    for fund_id in fund_ids:
        print(fund_id)
        fund_dividend_data = get_fund_all_split_data_for_save(fund_id)
        
        # 保存基金历史全部拆分到数据库
        cursor.executemany('''
            INSERT INTO fund_splits (fund_id, split_date, split_type, split_ratio)
            VALUES (?, ?, ?, ?)
        ''', fund_dividend_data)
        conn.commit()

    cursor.close()
    conn.close()


def get_fund_all_dividend_data_for_save(fund_id: str):
    fund_dividend_data = []
    fund_open_fund_info_em_df = ak.fund_open_fund_info_em(symbol=fund_id, indicator="分红送配详情")
    # print(fund_open_fund_info_em_df)
    if fund_open_fund_info_em_df is not None:
        fund_open_fund_info_em_df['每份分红'] = fund_open_fund_info_em_df['每份分红'].apply(lambda x: float(re.search(r'\d+\.\d+', x).group()) if re.search(r'\d+\.\d+', x) else None)
        fund_dividend_data = [(fund_id, row['除息日'], row['每份分红']) for index, row in fund_open_fund_info_em_df.iterrows()]
    return fund_dividend_data


# 待验证
def save_all_fund_dividend_data(funds_db_file_path: str):
    conn = sqlite3.connect(funds_db_file_path)
    cursor = conn.cursor()

    fund_ids = get_all_fund_id_asc_from_db()
    for fund_id in fund_ids:
        print(fund_id)
        fund_dividend_data = get_fund_all_dividend_data_for_save(fund_id)
        
        # 保存基金历史全部分红到数据库
        cursor.executemany('''
            INSERT INTO fund_dividends (fund_id, ex_dividend_date, dividend_per_share)
            VALUES (?, ?, ?)
        ''', fund_dividend_data)
        conn.commit()

    cursor.close()
    conn.close()


def get_fund_all_cumulative_nav_data(fund_id: str):
    # 获取基金的每日累计净值
    fund_cumulative_nav_data = []
    fund_open_fund_info_em_df = ak.fund_open_fund_info_em(symbol=fund_id, indicator="累计净值走势")
    if fund_open_fund_info_em_df is not None:
        fund_cumulative_nav_data = [(fund_id, row['净值日期'].isoformat(), row['累计净值']) for index, row in fund_open_fund_info_em_df.iterrows()]
    return fund_cumulative_nav_data


# 待验证
def save_all_fund_cumulative_nav_data(funds_db_file_path: str):
    conn = sqlite3.connect(funds_db_file_path)
    cursor = conn.cursor()

    fund_ids = get_all_fund_id_asc_from_db()

    for fund_id in fund_ids:
        print(fund_id)
        fund_cumulative_nav_data = get_fund_all_cumulative_nav_data(fund_id)

        # 保存基金历史全部每日单位净值到数据库
        cursor.executemany('''
            INSERT INTO fund_cumulative_nav (fund_id, value_date, cumulative_nav)
            VALUES (?, ?, ?)
        ''', fund_cumulative_nav_data)
        conn.commit()

    # conn.commit()
    cursor.close()
    conn.close()