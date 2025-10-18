import json
import requests
import time

class FeishuBitable:
    def __init__(self, app_id, app_secret, app_token, table_id, alpha_vantage_key=None):
        self.app_id = app_id
        self.app_secret = app_secret
        self.app_token = app_token
        self.table_id = table_id
        self.base_url = "https://open.feishu.cn/open-apis"
        self.access_token = None
        self.token_expiry_time = 0
        self._refresh_access_token()

        # Alpha Vantage API key配置（优先从环境变量读取，其次使用传入参数）
        import os
        self.alpha_vantage_key = os.environ.get('ALPHA_VANTAGE_API_KEY') or alpha_vantage_key

    def _refresh_access_token(self):
        """获取或刷新飞书API的访问令牌"""
        current_time = int(time.time())
        # 如果令牌不存在或即将过期（30秒内），则刷新
        if not self.access_token or current_time + 30 > self.token_expiry_time:
            self.access_token = self._get_access_token()
            # 默认访问令牌有效期为2小时
            self.token_expiry_time = current_time + 7200

    def _get_access_token(self):
        """获取飞书API的访问令牌"""
        url = f"{self.base_url}/auth/v3/tenant_access_token/internal/"
        headers = {"Content-Type": "application/json"}
        data = {
            "app_id": self.app_id,
            "app_secret": self.app_secret
        }

        response = requests.post(url, headers=headers, json=data)
        response_data = response.json()

        if response_data.get("code") == 0:
            return response_data.get("tenant_access_token")
        else:
            raise Exception(f"获取access_token失败: {response_data}")

    def _get_headers(self):
        """获取请求头，确保使用有效的access_token"""
        # 每次获取请求头时，确保access_token有效
        self._refresh_access_token()
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

    def search_records(self, filter=None, fields=None):
        """查询多维表格记录"""
        url = f"{self.base_url}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records/search"
        data = {}

        if filter:
            data["filter"] = filter

        if fields:
            data["field_names"] = fields

        response = requests.post(url, headers=self._get_headers(), json=data)
        response_data = response.json()

        if response_data.get("code") == 0:
            return response_data.get("data", {}).get("items", [])
        else:
            raise Exception(f"查询记录失败: {response_data}")

    def add_record(self, fields):
        """添加单条记录"""
        url = f"{self.base_url}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records"
        data = {
            "fields": fields
        }

        response = requests.post(url, headers=self._get_headers(), json=data)
        response_data = response.json()

        if response_data.get("code") == 0:
            return response_data.get("data", {})
        else:
            raise Exception(f"添加记录失败: {response_data}")

    def batch_add_records(self, records):
        """批量添加记录"""
        url = f"{self.base_url}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records/batch_create"
        data = {
            "records": [
                {"fields": record_fields} for record_fields in records
            ]
        }

        response = requests.post(url, headers=self._get_headers(), json=data)
        response_data = response.json()

        if response_data.get("code") == 0:
            return response_data.get("data", {})
        else:
            raise Exception(f"批量添加记录失败: {response_data}")

    def update_record(self, record_id, fields):
        """更新记录，添加更详细的错误处理"""
        url = f"{self.base_url}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records/{record_id}"
        data = {
            "fields": fields
        }

        print(f"尝试更新记录: record_id={record_id}, fields={fields}")
        print(f"请求URL: {url}")

        try:
            response = requests.put(url, headers=self._get_headers(), json=data)
            response_data = response.json()

            print(f"更新响应状态码: {response.status_code}")
            print(f"更新响应内容: {response_data}")

            if response_data.get("code") == 0:
                return response_data.get("data", {})
            else:
                # 根据不同的错误代码提供更具体的错误信息
                error_msg = f"更新记录失败: {response_data}"
                error_code = response_data.get("code")

                if error_code == 91403:
                    error_msg += " (权限不足，请检查应用权限设置)"
                elif error_code == 19021:
                    error_msg += " (访问令牌过期或无效)"
                elif error_code == 404:
                    error_msg += " (记录不存在或表格ID错误)"

                raise Exception(error_msg)
        except requests.exceptions.RequestException as e:
            raise Exception(f"HTTP请求异常: {str(e)}")

    def batch_update_records(self, records):
        """批量更新记录"""
        url = f"{self.base_url}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records/batch_update"
        data = {
            "records": records
        }

        print(f"批量更新 {len(records)} 条记录...")
        response = requests.post(url, headers=self._get_headers(), json=data)
        response_data = response.json()

        if response_data.get("code") == 0:
            print(f"批量更新成功: {len(records)} 条记录")
            return response_data.get("data", {})
        else:
            raise Exception(f"批量更新记录失败: {response_data}")

    def batch_update_records_by_code(self, code_records_dict, code_price_dict):
        """按代号批量更新记录，自动处理大量记录的分批更新"""
        total_updated = 0
        total_failed = 0

        print(f"\n开始批量更新，共{len(code_records_dict)}个代号需要更新")

        for code, record_list in code_records_dict.items():
            if code not in code_price_dict:
                print(f"代号 {code} 没有价格数据，跳过")
                continue

            print(f"\n代号 {code}: 更新 {len(record_list)} 条记录")

            # 准备批量更新的数据
            batch_records = []
            for record_info in record_list:
                batch_records.append({
                    "record_id": record_info["record_id"],
                    "fields": {
                        "last_price": code_price_dict[code]
                    }
                })

            try:
                # 执行批量更新
                updated = self.batch_update_records(batch_records)
                total_updated += len(record_list)
                print(f"代号 {code} 的 {len(record_list)} 条记录批量更新成功")
            except Exception as e:
                print(f"代号 {code} 批量更新失败: {e}")
                # 如果批量更新失败，尝试逐条更新
                print(f"尝试逐条更新代号 {code} 的记录...")
                for record_info in record_list:
                    try:
                        self.update_record(record_info["record_id"], {"last_price": code_price_dict[code]})
                        total_updated += 1
                        print(f"  记录 {record_info['record_id']} 更新成功")
                    except Exception as single_e:
                        total_failed += 1
                        print(f"  记录 {record_info['record_id']} 更新失败: {single_e}")

        print(f"\n批量更新完成: 成功 {total_updated} 条，失败 {total_failed} 条")
        return total_updated, total_failed

    def delete_record(self, record_id):
        """删除记录"""
        url = f"{self.base_url}/bitable/v1/apps/{self.app_token}/tables/{self.table_id}/records/{record_id}"

        response = requests.delete(url, headers=self._get_headers())
        response_data = response.json()

        if response_data.get("code") == 0:
            return True
        else:
            raise Exception(f"删除记录失败: {response_data}")

    def get_us_stock_price(self, ticker, exchange="XNAS"):
        """
        根据代号获取美股最新价格

        参数:
            ticker: 股票代号，如"AAPL"
            exchange: 交易所代码，默认为"XNAS"（纳斯达克），也可以是"XNYS"（纽交所）
                     注意：使用Alpha Vantage API时此参数不需要

        返回:
            float: 股票最新价格
        """
        # 优先使用Alpha Vantage API（免费额度：每天500次请求，每分钟5次）
        if hasattr(self, 'alpha_vantage_key') and self.alpha_vantage_key:
            try:
                url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={ticker}&apikey={self.alpha_vantage_key}"
                response = requests.get(url)
                response_data = response.json()

                # 检查响应是否成功
                if "Global Quote" in response_data and response_data["Global Quote"]:
                    quote = response_data["Global Quote"]
                    # 获取最新价格 (05. price字段)
                    latest_price = float(quote.get("05. price", 0))
                    if latest_price > 0:
                        return latest_price
                    else:
                        print(f"Alpha Vantage返回的价格无效: {response_data}")
                elif "Note" in response_data:
                    print(f"Alpha Vantage API达到速率限制: {response_data.get('Note')}")
                else:
                    print(f"Alpha Vantage返回数据格式异常: {response_data}")
            except Exception as e:
                print(f"使用Alpha Vantage获取股票{ticker}价格时发生错误: {e}")

        # 降级到tsanghi.com的免费API作为备用方案
        try:
            url = f"https://tsanghi.com/api/fin/stock/{exchange}/realtime?token=demo&ticker={ticker}"
            response = requests.get(url)
            response_data = response.json()

            # 检查响应是否成功
            if response_data.get("code") == 200 and "data" in response_data:
                stock_data = response_data["data"][0]
                # 获取最新价格
                latest_price = float(stock_data.get("close", 0))
                return latest_price
            else:
                print(f"获取股票{ticker}价格失败: {response_data}")
                return None
        except Exception as e:
            print(f"获取股票{ticker}价格时发生错误: {e}")
            return None

    def get_china_fund_price(self, fund_code):
        """
        根据代号获取A股基金的最新价格

        参数:
            fund_code: 基金代码，如"161725"（招商中证白酒指数）

        返回:
            float: 基金最新单位净值
        """
        # 尝试不同的交易所代码
        exchanges = [
            f"{fund_code}.SS",  # 上交所
            f"{fund_code}.SZ",  # 深交所
            f"{fund_code}"       # 无后缀
        ]

        # 尝试Yahoo Finance API的不同交易所代码
        for symbol in exchanges:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }

            try:
                response = requests.get(url, headers=headers)
                response_data = response.json()

                # 检查响应是否成功
                if response.status_code == 200 and "chart" in response_data and "result" in response_data["chart"] and response_data["chart"]["result"]:
                    result = response_data["chart"]["result"][0]
                    if "meta" in result and "regularMarketPrice" in result["meta"]:
                        latest_price = float(result["meta"]["regularMarketPrice"])
                        return latest_price
            except Exception as e:
                # 继续尝试下一个交易所代码
                continue

        # 如果Yahoo Finance API失败，尝试使用天天基金网作为备用数据源
        try:
            url = f"https://fund.eastmoney.com/pingzhongdata/{fund_code}.js"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }

            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                # 解析JS格式的数据
                content = response.text
                # 提取单位净值
                if "Data_netWorthTrend" in content:
                    # 查找最新净值数据
                    import re
                    match = re.search(r'var Data_netWorthTrend = \[(.*?)\];', content, re.DOTALL)
                    if match:
                        net_worth_data = json.loads("[" + match.group(1) + "]")
                        if net_worth_data and len(net_worth_data) > 0:
                            latest_data = net_worth_data[-1]
                            if "y" in latest_data:
                                return float(latest_data["y"])
        except Exception as e:
            print(f"使用天天基金网获取基金{fund_code}价格时发生错误: {e}")

        # 所有尝试都失败
        print(f"所有尝试获取基金{fund_code}价格均失败")
        return None

# 使用示例
if __name__ == "__main__":
    # 需要替换为您自己的app_id、app_secret、app_token和table_id
    # 这些信息可以在飞书开放平台和多维表格中获取
    app_id = "cli_a86134434cb6d00b"
    app_secret = "EKgc64XCkJGpZ9YQEq9JdeBqz4pQW2wi"
    app_token = "HSudbpG6vaPDV2s8VyTcexUvnNd"
    table_id = "tblWkoXwMbKGcQ6w"

    # Alpha Vantage API key（可选）
    # 方式1: 从环境变量读取 ALPHA_VANTAGE_API_KEY
    # 方式2: 直接传入参数
    # alpha_vantage_key = "YOUR_API_KEY_HERE"  # 在 https://www.alphavantage.co/support/#api-key 免费获取
    alpha_vantage_key = 'M4LKZ1W3LXK1DAUH'  # 不使用时设为None，将使用备用API

    try:
        # 初始化客户端
        bitable = FeishuBitable(app_id, app_secret, app_token, table_id, alpha_vantage_key)

        # 示例：查询记录
        print("查询记录示例:")
        records = bitable.search_records()
        print(f"找到 {len(records)} 条记录")

        # 提取所有代号并去重
        code_set = set()  # 使用集合自动去重

        for record in records:
            # 检查记录中是否有"代号"字段
            if record.get("fields") and "代号" in record.get("fields"):
                code_field = record.get("fields").get("代号")
                # 检查代号字段是否为列表类型
                if isinstance(code_field, list):
                    # 遍历列表中的所有代号项
                    for code_item in code_field:
                        # 检查代号项中是否有"text"字段
                        if isinstance(code_item, dict) and "text" in code_item:
                            code = code_item.get("text")
                            if code:  # 确保代号不为空
                                code_set.add(code)

        # 将集合转换为列表
        unique_codes = list(code_set)

        # 创建字典用于存储代号和对应的价格
        code_price_dict = {}
        # code_price_dict = {'BBAI': 7.22, 'SMH': 325.34, 'PSTV': 0.6973, 'RXRX': 5.32, 'SOUN': 17.36, '159599': 2.19, '588780': 1.622, 'AIQ': 48.93, 'ADEA': 15.71, '512710': 0.736, 'BIDU': 121.69, '561910': 0.799, '588200': 2.527, 'AMZN': 216.37, 'META': 705.3, 'BABA': 159.01, 'JD': 31.85, '588720': 1.481, 'GOOG': 237.49, '512160': 1.448, '161903': 1.431, 'APP': 569.89, 'TCEHY': 79.96, '110003': 2.0998, '588790': 0.843, 'VGT': 736.33}

        print(f"去重后的代号数量: {len(unique_codes)}")
        print("去重后的代号列表:")
        for code in unique_codes:
            # continue
            print(code, end="\t")
            # 根据代号是否为数字来决定调用哪个函数获取价格
            if code and code.isdigit():
                # 如果代号是数字，调用get_china_fund_price获取A股基金价格
                fund_price = bitable.get_china_fund_price(code)
                if fund_price is not None:
                    print(f"  A股基金最新净值: ¥{fund_price:.4f}")
                    code_price_dict[code] = fund_price
                else:
                    print("  获取A股基金价格失败")
            else:
                # 如果代号不是数字，调用get_us_stock_price获取美股价格
                stock_price = bitable.get_us_stock_price(code)
                if stock_price is not None:
                    print(f"  美股最新价格: ${stock_price:.2f}")
                    code_price_dict[code] = stock_price
                else:
                    print("  获取美股价格失败")

        # 打印字典内容以供验证
        print("代号价格字典:")
        print(code_price_dict)

        # 示例：添加记录
        # new_record = {"标题": "测试标题", "内容": "测试内容"}
        # added = bitable.add_record(new_record)
        # print(f"添加记录成功: {added}")

        # 示例：批量添加记录
        # batch_records = [
        #     {"标题": "批量测试1", "内容": "批量内容1"},
        #     {"标题": "批量测试2", "内容": "批量内容2"}
        # ]
        # batch_added = bitable.batch_add_records(batch_records)
        # print(f"批量添加记录成功: {batch_added}")

        # 示例：更新记录
        # if records:
        #     record_id = records[0]["record_id"]
        #     updated = bitable.update_record(record_id, {"标题": "已更新的标题"})
        #     print(f"更新记录成功: {updated}")

        # 示例：删除记录
        # if records:
        #     record_id = records[0]["record_id"]
        #     deleted = bitable.delete_record(record_id)
        #     print(f"删除记录成功: {deleted}")

        # 按代号分组记录，准备批量更新
        code_records_dict = {}  # 代号 -> 记录列表的映射

        for record in records:
            if record.get("fields") and "代号" in record.get("fields"):
                code_field = record.get("fields").get("代号")
                record_id = record.get("record_id")

                # 处理代号字段（可能是列表）
                if isinstance(code_field, list) and len(code_field) > 0:
                    code = code_field[0].get("text")
                else:
                    continue

                if code and isinstance(code, str) and code in code_price_dict:
                    if code not in code_records_dict:
                        code_records_dict[code] = []
                    code_records_dict[code].append({
                        "record_id": record_id
                    })

        # 使用新的批量更新方法
        total_updated, total_failed = bitable.batch_update_records_by_code(code_records_dict, code_price_dict)

    except Exception as e:
        print(f"发生错误: {e}")