import os

import datetime
from bson.code import Code
from mongodb_connect import Mongodb_Connect

ip_files = ["web_sales", "web_returns", "warehouse", "item", "date_dim"]


class Price_Optimizationn_Data_Mongodb(object):
    def __init__(self):
        self.client = Mongodb_Connect().get_client()
        self.db = self.client['PriceOptimizationMini']

    def load_data(self, file_name, f):
        file = open(file_name)
        for line in file:
            attributes = line.split('|')
            if f == 'warehouse':
                self.db.warehouse.insert_one({
                    'w_warehouse_sk': attributes[0],
                    'w_warehouse_id': attributes[1],
                    'w_warehouse_name': attributes[2],
                    'w_warehouse_sq_ft': attributes[3],
                    'w_street_number': attributes[4],
                    'w_street_name': attributes[5],
                    'w_street_type': attributes[6],
                    'w_suite_number': attributes[7],
                    'w_city': attributes[8],
                    'w_county': attributes[9],
                    'w_state': attributes[10],
                    'w_zip': attributes[11],
                    'w_country': attributes[12],
                    'w_gmt_offset': attributes[13]
                })
            elif f == 'web_sales':
                self.db.web_sales.insert_one({
                    'ws_sold_date_sk': attributes[0],
                    'ws_sold_time_sk': attributes[1],
                    'ws_ship_date_sk': attributes[2],
                    'ws_item_sk': attributes[3],
                    'ws_bill_customer_sk': attributes[4],
                    'ws_bill_cdemo_sk': attributes[5],
                    'ws_bill_hdemo_sk': attributes[6],
                    'ws_bill_addr_sk': attributes[7],
                    'ws_ship_customer_sk': attributes[8],
                    'ws_ship_cdemo_sk': attributes[9],
                    'ws_ship_hdemo_sk': attributes[10],
                    'ws_ship_addr_sk': attributes[11],
                    'ws_web_page_sk': attributes[12],
                    'ws_web_site_sk': attributes[13],
                    'ws_ship_mode_sk': attributes[14],
                    'ws_warehouse_sk': attributes[15],
                    'ws_promo_sk': attributes[16],
                    'ws_order_number': attributes[17],
                    'ws_quantity': attributes[18],
                    'ws_wholesale_cost': attributes[19],
                    'ws_list_price': attributes[20],
                    'ws_sales_price': attributes[21],
                    'ws_ext_discount_amt': attributes[22],
                    'ws_ext_sales_price': attributes[23],
                    'ws_ext_wholesale_cost': attributes[24],
                    'ws_ext_list_price': attributes[25],
                    'ws_ext_tax': attributes[26],
                    'ws_coupon_amt': attributes[27],
                    'ws_ext_ship_cost': attributes[28],
                    'ws_net_paid': attributes[29],
                    'ws_net_paid_inc_tax': attributes[30],
                    'ws_net_paid_inc_ship': attributes[31],
                    'ws_net_paid_inc_ship_tax': attributes[32],
                    'ws_net_profit': attributes[33],
                })
            elif f == 'web_returns':
                self.db.web_returns.insert_one({
                    'wr_returned_date_sk': attributes[0],
                    'wr_returned_time_sk': attributes[1],
                    'wr_item_sk': attributes[2],
                    'wr_refunded_customer_sk': attributes[3],
                    'wr_refunded_cdemo_sk': attributes[4],
                    'wr_refunded_hdemo_sk': attributes[5],
                    'wr_refunded_addr_sk': attributes[6],
                    'wr_returning_customer_sk': attributes[7],
                    'wr_returning_cdemo_sk': attributes[8],
                    'wr_returning_hdemo_sk': attributes[9],
                    'wr_returning_addr_sk': attributes[10],
                    'wr_web_page_sk': attributes[11],
                    'wr_reason_sk': attributes[12],
                    'wr_order_number': attributes[13],
                    'wr_return_quantity': attributes[14],
                    'wr_return_amt': attributes[15],
                    'wr_return_tax': attributes[16],
                    'wr_return_amt_inc_tax': attributes[17],
                    'wr_fee': attributes[18],
                    'wr_return_ship_cost': attributes[19],
                    'wr_refunded_cash': attributes[20],
                    'wr_reversed_charge': attributes[21],
                    'wr_account_credit': attributes[22],
                    'wr_net_loss': attributes[23]
                })
            elif f == 'item':
                self.db.item.insert_one({
                    'i_item_sk': attributes[0],
                    'i_item_id': attributes[1],
                    'i_rec_start_': attributes[2],
                    'i_rec_end_': attributes[3],
                    'i_item_desc': attributes[4],
                    'i_current_price': attributes[5],
                    'i_wholesale_cost': attributes[6],
                    'i_brand_id': attributes[7],
                    'i_brand': attributes[8],
                    'i_class_id': attributes[9],
                    'i_class': attributes[10],
                    'i_category_id': attributes[11],
                    'i_category': attributes[12],
                    'i_manufact_id': attributes[13],
                    'i_manufact': attributes[14],
                    'i_size': attributes[15],
                    'i_formulation': attributes[16],
                    'i_color': attributes[17],
                    'i_units': attributes[18],
                    'i_container': attributes[19],
                    'i_manager_id': attributes[20],
                    'i_product_name': attributes[21]
                })
            elif f == 'date_dim':
                self.db.date_dim.insert_one({
                    'd_date_sk': attributes[0],
                    'd_date_id': attributes[1],
                    'd_date': attributes[2],
                    'd_month_seq': attributes[3],
                    'd_week_seq': attributes[4],
                    'd_quarter_seq': attributes[5],
                    'd_year': attributes[6],
                    'd_dow': attributes[7],
                    'd_moy': attributes[8],
                    'd_dom': attributes[9],
                    'd_qoy': attributes[10],
                    'd_fy_year': attributes[11],
                    'd_fy_quarter_seq': attributes[12],
                    'd_fy_week_seq': attributes[13],
                    'd_day_name': attributes[14],
                    'd_quarter_name': attributes[15],
                    'd_holiday': attributes[16],
                    'd_weekend': attributes[17],
                    'd_following_holiday': attributes[18],
                    'd_first_dom': attributes[19],
                    'd_last_dom': attributes[20],
                    'd_same_day_ly': attributes[21],
                    'd_same_day_lq': attributes[22],
                    'd_current_day': attributes[23],
                    'd_current_week': attributes[24],
                    'd_current_month': attributes[25],
                    'd_current_quarter': attributes[26],
                    'd_current_year': attributes[27]
                })

        file.close()

    def get_review(self, key):
        result = self.db.product_reviews.find_one({'id': key})
        return result['review']

    def delete_docs(self):
        self.db.product_reviews.drop()


def merge_two_dicts(x, y):
    z = x.copy()
    z.update(y)
    return z
    pass


def within_date_range(base_date, check_date, num_days):
    lower_limit = base_date - datetime.timedelta(days=num_days)
    upper_limit = base_date + datetime.timedelta(days=num_days)
    if lower_limit <= check_date <= upper_limit:
        return True
    else:
        return False
    pass

# Performs SQL SUM
reducefuction = Code("""
                        function(obj, prev){
                            prev.sales_before = String(parseInt(prev.sales_before, 10)  + parseInt(obj.ws_sales_price,10) - 0);
                            prev.sales_after = String(parseInt(prev.sales_after, 10)  + parseInt(obj.ws_sales_price,10) - 0);
                            }
                        """)

if __name__ == "__main__":
    os.chdir("..")
    global ip_files
    po_obj = Price_Optimizationn_Data_Mongodb()
    for f in ip_files:
        ip_file_path = os.path.abspath(os.curdir) + "\input\\" + f + ".dat"
        po_obj.load_data(ip_file_path, f)
    pass

    # Join operation begin
    web_sales = po_obj.db.get_collection('web_sales_mini')
    web_returns = po_obj.db.get_collection('web_returns_mini')
    po_obj.db.create_collection('web_sales_LOJ_web_returns_mini')

    for i in web_sales.find():
        for j in web_returns.find():
            if i['ws_item_sk'] == j['wr_item_sk'] and i['ws_item_sk'] == j['wr_item_sk']:
                try:
                    del i['_id']
                except KeyError:
                    pass
                try:
                    del j['_id']
                except KeyError:
                    pass

                po_obj.db.web_sales_LOJ_web_returns_mini.insert_one(merge_two_dicts(i, j))
    # # Join operation end



    # WHERE clause begin
    web_sales_LOJ_web_returns = po_obj.db.get_collection('web_sales_LOJ_web_returns_mini')
    if 'web_sales_LOJ_web_returns_where_clause_result_mini' in po_obj.db.collection_names():
        po_obj.db.drop_collection('web_sales_LOJ_web_returns_where_clause_result_mini')
    web_sales_LOJ_web_returns_where_clause_result_mini = po_obj.db.create_collection(
        'web_sales_LOJ_web_returns_where_clause_result_mini')
    warehouse_mini = po_obj.db.get_collection('warehouse')
    item_mini = po_obj.db.get_collection('item')
    date_dim_mini = po_obj.db.get_collection('date_dim')
    count = 0
    # print 'For loop start...'
    row = 1
    for i in web_sales_LOJ_web_returns.find({},{"ws_item_sk":1,"ws_warehouse_sk":1,"ws_sold_date_sk":1,"ws_sales_price":1,"wr_refunded_cash":1,"_id":0}):
        print row
        row = row+1
        for j in item_mini.find({},{"i_item_sk":1,"i_item_id":1,"_id":0}):
            if i['ws_item_sk'] == j['i_item_sk']:
                for k in warehouse_mini.find({},{"w_warehouse_sk":1,"w_state":1,"_id":0}):
                    if i['ws_warehouse_sk'] == k['w_warehouse_sk']:
                        for l in date_dim_mini.find({},{"d_date_sk":1,"d_date":1,"_id":0}):
                            if i['ws_sold_date_sk'] == l['d_date_sk']:
                                base_date = datetime.datetime.strptime('2001-01-01', '%Y-%m-%d')
                                check_date = datetime.datetime.strptime(l['d_date'],'%Y-%m-%d')
                                if within_date_range(base_date,check_date,30):
                                    attributes = [k['w_state'], j['i_item_id'], l['d_date'], i['ws_sales_price'],
                                                  i['wr_refunded_cash']]
                                    web_sales_LOJ_web_returns_where_clause_result_mini.insert_one({
                                        'w_state': attributes[0],
                                        'i_item_id': attributes[1],
                                        'd_date': attributes[2],
                                        'ws_sales_price': attributes[3],
                                        'wr_refunded_cash': attributes[4]
                                    })
                                    count = count + 1
                                    if count % 100 == 0:
                                        print 'Found ' + str(count) + ' records'
                                        # break
    # WHERE clause end


    # results = po_obj.db.things.group(key={"x":1}, condition={}, initial={"count": 0}, reduce=reducer)
    res = po_obj.db.web_sales_LOJ_web_returns_where_clause_result_mini.group(
        condition = {},
        key={
            "w_state": 1,
            "i_item_id": 1
        },
        initial={
            "sales_before":0,
            "sales_after":0
        },
        reduce = reducefuction
        # sales_after = co
    )
    print res

"""
PostgreSQL query:
SELECT
w_state , i_item_id
,sum(case when (cast(d_date as date) <
cast ('1998-03-16' as date))
then ws_sales_price -
coalesce (wr_refunded_cash ,0)
else 0 end)
as sales_before
,sum(case when (cast(d_date as date) >=
cast ('1998-03-16' as date))
then ws_sales_price -
coalesce (wr_refunded_cash ,0)
else 0 end) as sales_after
FROM
web_sales left outer join web_returns on
(ws_order_number = wr_order_number
and ws_item_sk = wr_item_sk)
,warehouse , item , date_dim
WHERE
i_item_sk = ws_item_sk
and ws_warehouse_sk = w_warehouse_sk
and ws_sold_date_sk = d_date_sk
and d_date between
(cast ('1998-03-16' as date) - interval '30
day')
and (cast ('1998-03-16' as date) + interval
'30 day')
GROUP by w_state ,i_item_id
ORDER by w_state ,i_item_id;
"""
