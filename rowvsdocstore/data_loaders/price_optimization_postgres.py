import os
import sys

from psycopg2._psycopg import ProgrammingError
from postgres_connect import Postgres_Connect
from price_optimization_relations import Price_Optimization_Relations

class Price_optimization_postgres(object):

    def __init__(self):
        self.con = Postgres_Connect().getConnection('cse591prj' , 'postgres', 'localhost', 'ok')
        self.cur = self.con.cursor()
        self.ip_files = ["web_sales","web_returns","warehouse","item","date_dim"]
        self.queries = Price_Optimization_Relations()
        self.logging = True

    def load_data(self):

        try:
            if self.logging:
                print("Start")
            os.chdir("..")
            if self.logging:
                print("imporing data..")

            self.cur.execute(self.queries.web_sales)
            self.cur.execute(self.queries.web_returns)
            self.cur.execute(self.queries.warehouse)
            self.cur.execute(self.queries.date_dim)
            self.cur.execute(self.queries.item)

            self.cur.execute(self.queries.alter_1)
            self.cur.execute(self.queries.alter_2)


            for f in self.ip_files:
                if self.logging:
                    print("importing " + f)
                ip_file_path = '/home/master/Desktop/cse591_adb_project/rowvsdocstore/data/' + f + ".dat"
                '''
                Folder permissions needed to execute copy_expert. If running on windows system,
                navigate to ip_file_path in command prompt and run following commands:
                takeown /r /d y /f *
                icacls * /t  /grant Everyone:F
                '''
                #COPY warehouse FROM '/home/master/Desktop/cse591_adb_project/rowvsdocstore/data/warehouse.dat' DELIMITER '|';
                query = "COPY " + f + " FROM '" + ip_file_path + "' DELIMITER '|' CSV HEADER;"
                print(query)
                self.cur.execute(query)
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
        
        drop_query = "DROP TABLE IF EXISTS "
        for table in self.ip_files:
            drop_query += table+", "

        drop_query += " CASCADE"
        self.cur.execute(drop_query)
        self.con.commit()
        if self.logging:
            print("Tables are deleted!")

