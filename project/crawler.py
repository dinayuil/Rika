import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from io import StringIO

def get_fund_basic_info(fund_code: str):
    """获取基金基础信息

    Args:
        fund_code (str): 基金代码

    Returns:
        dict: 
            '基金代码': str
            '基金简称': str
            '类型':  (str)
            '规模':  (str)
            '成立日': YYYY-MM-DD (str)
            '封闭期': 以月份为单位 (int)
            '申购状态': (str)
            '预估开放时间': YYYY-MM-DD/YYYY-MM-DD 分别为开放申购和赎回日期(str)
            '申购费率': 最高一档手续费 (str)
            '基金经理': (str)
            '最新经理变动日期': YYYY-MM-DD (str)
            '最低赎回费率适用期限': str,
            '最低赎回费率': str
    """

    # 目标URL
    url = f'http://fund.eastmoney.com/{fund_code}.html'

    try:
        # 发送GET请求
        response = requests.get(url)
        response.encoding = 'utf-8'
    except Exception as e:
        raise Exception(f"Error fetching {url} for fund code {fund_code}: {e}")
    
    try:
        # 解析HTML
        soup = BeautifulSoup(response.text, 'html.parser')
    except Exception as e:
        raise Exception(f"Error parsing HTML for fund code {fund_code}: {e}")
    
    try:
        fund_name = soup.find('div', class_='fundDetail-tit').div.get_text(strip=True).split('(')[0]
        fund_info = {
            '基金简称': fund_name
        }
    except Exception as e:
        raise Exception(f"Error finding fund name div for fund code {fund_code}: {e}")

    try:
        # 找到包含基金信息的div
        info_div = soup.find('div', class_='infoOfFund')
    except Exception as e:
        raise Exception(f"Error finding info div for fund code {fund_code}: {e}")
    
    try:
        # 找到div中的表格
        table = info_div.find('table')
        # 使用pandas读取表格
        df = pd.read_html(StringIO(str(table)))[0]
    except Exception as e:
        raise Exception(f"Error reading basic info table for fund code {fund_code}: {e}")
    
    # print(df)

    try:
        # 提取所需信息
        fund_info['类型'] = df.iloc[0, 0].split('：')[1].split('|')[0].strip()
        fund_info['规模'] = re.search(r'\d+\.\d+', df.iloc[0, 1]).group()
        fund_info['成立日'] = df.iloc[1, 0].split('：')[1].strip()
    except Exception as e:
        raise Exception(f"Error extracting basic info for fund code {fund_code}: {e}")

    try:
        # 检查是否存在“封闭期”字段并提取
        closure_period = table.find('td', string=lambda x: x and '封闭期' in x)
        if closure_period:
            closure_text = closure_period.text.split('：')[1].strip()
            # 转换封闭期为月份
            if '年' in closure_text:
                months = int(re.search(r'\d+', closure_text).group()) * 12
            elif '个月' in closure_text:
                months = int(re.search(r'\d+', closure_text).group())
            else:
                months = '未知'
            fund_info['封闭期'] = months
        else:
            fund_info['封闭期'] = 0
    except Exception as e:
        raise Exception(f"Error extracting closure period for fund code {fund_code}: {e}")
    
    try:
        # 获取交易信息
        buy_way_div = soup.find('div', class_='buyWayStatic')
        trade_info = buy_way_div.find_all('div', class_='staticItem')

        # 提取申购状态
        # trade_status = buy_way_div.find('div', class_='staticItem').find('span', class_='staticCell')
        trade_status = trade_info[0].find('span', class_='staticCell')
        fund_info['申购状态'] = trade_status.text.strip()

        # 提取预估开放申购/赎回时间
        estimated_open_time = trade_info[2].find('span', class_='ui-color-red planData kfadate')
        if estimated_open_time:
            estimated_open_time = estimated_open_time.text.strip()
        else:
            estimated_open_time = ''
        fund_info['预估开放时间'] = estimated_open_time    

        # 提取购买手续费
        fee_info = trade_info[4].find('span', class_='comparePrice').text.strip()
        if fee_info == '':
            fee_info = trade_info[4].find('span', class_='nowPrice').text.strip()
        fund_info['申购费率'] = fee_info
    except Exception as e:
        raise Exception(f"Error extracting trade info for fund code {fund_code}: {e}")
    

    try:
        # 提取基金经理和任职时间信息
        manager_tab = soup.find('li', class_='fundManagerTab')
        manager_table = manager_tab.find('table')
        manager_df = pd.read_html(StringIO(str(manager_table)))[0]

        # 获取第一行的基金经理和任职时间信息
        first_row = manager_df.iloc[0]
        latest_manager_change_date = first_row['任职时间'].split('~')[0].strip()
        fund_info['基金经理'] = first_row['基金经理']
        fund_info['最新经理变动日期'] = latest_manager_change_date
    except Exception as e:
        raise Exception(f"Error extracting manager info for fund code {fund_code}: {e}")
    
    # 获取赎回费用信息
    redemption_info = get_least_redemption_period_rate(fund_code, '货币型' in fund_info['类型'])

    fund_info = fund_info | redemption_info
    fund_info['基金代码'] = fund_code

    return fund_info


def get_least_redemption_period_rate(fund_code: str, is_money_fund: bool):
    """获取基金最低赎回费率和对应须持有的时长

    Args:
        fund_code (str): 基金代码
        is_money_fund (bool): 是否为货币基金
    Raises:
        Exception: 获取网页内容出错
        Exception: 解析网页出错
        Exception: 解析并提取赎回费用出错
        Exception: 适用期限转换出错

    Returns:
        dict:
            '最低赎回费率适用期限': str,
            '最低赎回费率': str
    """
    # 如果是货币基金则不获取，货基一般不收取赎回费用，也没有适用期限
    if is_money_fund:
        return {
            '最低赎回费率适用期限': '',
            '最低赎回费率': ''
            }

    try:
        # 获取网页内容
        url = f"http://fundf10.eastmoney.com/jjfl_{fund_code}.html"
        response = requests.get(url)
        response.encoding = 'utf-8' 
    except Exception as e:
        raise Exception(f"Error fetching {url} for fund code {fund_code}: {e}")
    
    try:
        # 解析网页内容
        soup = BeautifulSoup(response.text, 'html.parser')
    except Exception as e:
        raise Exception(f"Error parsing HTML for fund code {fund_code}: {e}")
    
    try:
        # 找到赎回费用表格
        table = soup.find_all('table', class_='w650 comm jjfl')[-1]

        # 提取表格最后一行数据
        last_row = table.find_all('tr')[-1]
        columns = last_row.find_all('td')

        # 获取数据
        # applicable_amount = columns[0].text.strip()
        applicable_period = columns[1].text.strip()
        redemption_rate = columns[2].text.strip()
    except Exception as e:
        raise Exception(f"Error extracting redemption table info for fund code {fund_code}: {e}")
    
    try:
        # 转换适用期限为天
        number_of_date_match = re.search(r'(\d+)', applicable_period)
        if '天' in applicable_period:
            applicable_period = number_of_date_match.group(1)
        elif '年' in applicable_period:
            applicable_period = str(int(number_of_date_match.group(1)) * 365)
        else:
            assert(False)
    except Exception as e:
        raise Exception(f"Error converting applicable period to days for fund code {fund_code}: {e}")
    
    redemption_info = {
        '最低赎回费率适用期限': applicable_period,
        '最低赎回费率': redemption_rate
    }
    return redemption_info

# test
# print(get_fund_basic_info('018647'))
# print(get_least_redemption_period_rate('050025'))