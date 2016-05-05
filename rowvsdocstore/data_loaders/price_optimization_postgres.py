import os
import sys

from psycopg2._psycopg import ProgrammingError
from postgres_connect import Postgres_Connect

class Price_optimization_postgres(object):

    def __init__(self):
        self.con = Postgres_Connect().getConnection('cse591' , 'postgres', 'localhost', 'root')
        self.cur = self.con.cursor()
        self.ip_files = ["web_sales","web_returns","warehouse","item","date_dim"]
        self.logging = True

    def load_data(self):

        try:
            if self.logging:
                print("Start")
            os.chdir("..")
            if self.logging:
                print("imporing data..")
            for f in self.ip_files:
                if self.logging:
                    print("importing " + f)
                ip_file_path = os.path.abspath(os.curdir) + "\input\\" + f + ".dat"
                '''
                Folder permissions needed to execute copy_expert. If running on windows system,
                navigate to ip_file_path in command prompt and run following commands:
                takeown /r /d y /f *
                icacls * /t  /grant Everyone:F
                '''
                query = "COPY " + f + " FROM '" + ip_file_path + "' DELIMITER '|' CSV HEADER"
                self.cur.copy_expert(query, sys.stdin)
                self.con.commit()
                if self.logging:
                    print("Finished importing " + f + ". Committed!")
            return 0
        except ProgrammingError:
            if self.logging:
                print("Something went wrong with the COPY function:\n ")
            return 1
        except:
            return 2

    def execute_query(self):
        query = """SELECT w_state , i_item_id
                ,sum(case when (cast(d_date as date) <
                cast ('2001-03-16' as date))
                then ws_sales_price -
                     coalesce (wr_refunded_cash ,0)
                else 0 end)
                as sales_before
                ,sum(case when (cast(d_date as date) >=
                cast ('2001-03-16' as date))
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
                (cast ('2001-03-16' as date) - interval '30
                day')
                   and (cast ('2001-03-16' as date) + interval
                '30 day')
                GROUP by w_state ,i_item_id
                ORDER by w_state ,i_item_id;"""
        self.cur.execute(query)
        return self.cur.fetchall()

    def delete_schema(self):
        global connection,ip_files,cursor,connection_string
        drop_query = "DROP TABLE IF EXISTS "
        for table in ip_files:
            drop_query += table+", "

        drop_query += " CASCADE"
        self.cur.execute(drop_query)
        self.con.commit()
        if self.logging:
            print("Tables are deleted!")
        pass

    """
    Following are the queries used to create tables in PostgreSQL database:

    create table web_sales
    (
     ws_sold_date_sk integer ,
     ws_sold_time_sk integer ,
     ws_ship_date_sk integer ,
     ws_item_sk integer not null,
     ws_bill_customer_sk integer ,
     ws_bill_cdemo_sk integer ,
     ws_bill_hdemo_sk integer ,
     ws_bill_addr_sk integer ,
     ws_ship_customer_sk integer ,
     ws_ship_cdemo_sk integer ,
     ws_ship_hdemo_sk integer ,
     ws_ship_addr_sk integer ,
     ws_web_page_sk integer ,
     ws_web_site_sk integer ,
     ws_ship_mode_sk integer ,
     ws_warehouse_sk integer ,
     ws_promo_sk integer ,
     ws_order_number integer not null,
     ws_quantity integer ,
     ws_wholesale_cost decimal(7,2) ,
     ws_list_price decimal(7,2) ,
     ws_sales_price decimal(7,2) ,
     ws_ext_discount_amt decimal(7,2) ,
     ws_ext_sales_price decimal(7,2) ,
     ws_ext_wholesale_cost decimal(7,2) ,
     ws_ext_list_price decimal(7,2) ,
     ws_ext_tax decimal(7,2) ,
     ws_coupon_amt decimal(7,2) ,
     ws_ext_ship_cost decimal(7,2) ,
     ws_net_paid decimal(7,2) ,
     ws_net_paid_inc_tax decimal(7,2) ,
     ws_net_paid_inc_ship decimal(7,2) ,
     ws_net_paid_inc_ship_tax decimal(7,2) ,
     ws_net_profit decimal(7,2) ,
     primary key (ws_item_sk, ws_order_number)
    );

    create table web_returns
    (
     wr_returned_date_sk integer ,
     wr_returned_time_sk integer ,
     wr_item_sk integer not null,
     wr_refunded_customer_sk integer ,
     wr_refunded_cdemo_sk integer ,
     wr_refunded_hdemo_sk integer ,
     wr_refunded_addr_sk integer ,
     wr_returning_customer_sk integer ,
     wr_returning_cdemo_sk integer ,
     wr_returning_hdemo_sk integer ,
     wr_returning_addr_sk integer ,
     wr_web_page_sk integer ,
     wr_reason_sk integer ,
     wr_order_number integer not null,
     wr_return_quantity integer ,
     wr_return_amt decimal(7,2) ,
     wr_return_tax decimal(7,2) ,
     wr_return_amt_inc_tax decimal(7,2) ,
     wr_fee decimal(7,2) ,
     wr_return_ship_cost decimal(7,2) ,
     wr_refunded_cash decimal(7,2) ,
     wr_reversed_charge decimal(7,2) ,
     wr_account_credit decimal(7,2) ,
     wr_net_loss decimal(7,2) ,
     primary key (wr_item_sk, wr_order_number)
    );

    create table warehouse
    (
     w_warehouse_sk integer not null,
     w_warehouse_id char(16) not null,
     w_warehouse_name varchar(20) ,
     w_warehouse_sq_ft integer ,
     w_street_number char(10) ,
     w_street_name varchar(60) ,
     w_street_type char(15) ,
     w_suite_number char(10) ,
     w_city varchar(60) ,
     w_county varchar(30) ,
     w_state char(2) ,
     w_zip char(10) ,
     w_country varchar(20) ,
     w_gmt_offset decimal(5,2) ,
     primary key (w_warehouse_sk)
    );

    create table item
    (
     i_item_sk integer not null,
     i_item_id char(16) not null,
     i_rec_start_date date ,
     i_rec_end_date date ,
     i_item_desc varchar(200) ,
     i_current_price decimal(7,2) ,
     i_wholesale_cost decimal(7,2) ,
     i_brand_id integer ,
     i_brand char(50) ,
     i_class_id integer ,
     i_class char(50) ,
     i_category_id integer ,
     i_category char(50) ,
     i_manufact_id integer ,
     i_manufact char(50) ,
     i_size char(20) ,
     i_formulation char(20) ,
     i_color char(20) ,
     i_units char(10) ,
     i_container char(10) ,
     i_manager_id integer ,
     i_product_name char(50) ,
     primary key (i_item_sk)
    );


    create table date_dim
    (
     d_date_sk integer not null,
     d_date_id char(16) not null,
     d_date date ,
     d_month_seq integer ,
     d_week_seq integer ,
     d_quarter_seq integer ,
     d_year integer ,
     d_dow integer ,
     d_moy integer ,
     d_dom integer ,
     d_qoy integer ,
     d_fy_year integer ,
     d_fy_quarter_seq integer ,
     d_fy_week_seq integer ,
     d_day_name char(9) ,
     d_quarter_name char(6) ,
     d_holiday char(1) ,
     d_weekend char(1) ,
     d_following_holiday char(1) ,
     d_first_dom integer ,
     d_last_dom integer ,
     d_same_day_ly integer ,
     d_same_day_lq integer ,
     d_current_day char(1) ,
     d_current_week char(1) ,
     d_current_month char(1) ,
     d_current_quarter char(1) ,
     d_current_year char(1) ,
     primary key (d_date_sk)
    );

    # CONSTRAINT modifications:

    alter table web_sales drop constraint web_sales_pkey;
    alter table web_returns drop constraint web_returns_pkey;

    # COPY queries:

    COPY web_sales FROM  'C:\\Users\\Charan\\IdeaProjects\\CSE591_ADS_Project\\rowvsdocstore\\input\\web_sales.dat'  DELIMITER '|' CSV HEADER;
    COPY web_returns FROM  'C:\\Users\\Charan\\IdeaProjects\\CSE591_ADS_Project\\rowvsdocstore\\input\\web_returns.dat'  DELIMITER '|' CSV HEADER;
    COPY warehouse FROM  'C:\\Users\\Charan\\IdeaProjects\\CSE591_ADS_Project\\rowvsdocstore\\input\\warehouse.dat'  DELIMITER '|' CSV HEADER;
    COPY item FROM  'C:\\Users\\Charan\\IdeaProjects\\CSE591_ADS_Project\\rowvsdocstore\\input\\item.dat'  DELIMITER '|' CSV HEADER;
    COPY date_dim FROM  'C:\\Users\\Charan\\IdeaProjects\\CSE591_ADS_Project\\rowvsdocstore\\input\\date_dim.dat'  DELIMITER '|' CSV HEADER;

    """

