# -*- coding: utf-8 -*-
if __name__ == '__main__':
    print("EDA를 진행합니다.")

import sqlite3

class db_eda():
    def __init__(self):
        conn = sqlite3.connect('raw_data_2016_2021.db')
        self.conn = conn
        cur = conn.cursor()
        self.cur = cur
    
    def make_sales_2(self):
        # sales 테이블에서 상권명으로 묶음
        self.cur.execute('''
            CREATE TABLE sales_2 AS SELECT s.YYYY, s.Q, S.상권_구분명, s.상권명, count(*), sum(sales), sum(점포수)
            FROM sales s 
            GROUP BY YYYY, Q, 상권명
            ORDER BY 상권명''')
        self.conn.commit()

    def make_join_table(self):
        # sales_2 테이블과 living, working, house 테이블 join
        self.cur.execute('''
            CREATE TABLE FINAL_TABLE AS SELECT "source"."YYYY" AS "YYYY", "source"."Q" AS "Q", "source"."상권_구분명" AS "상권_구분명", "source"."상권명" AS "상권명", "source"."업종수" AS "업종수", "source"."점포수" AS "점포수", "source"."총_생활인구" AS "총_생활인구", "source"."총_직장인구" AS "총_직장인구", "source"."아파트_단지수" AS "아파트_단지수", "source"."아파트_평균_시가" AS "아파트_평균_시가", "source"."총매출" AS "총매출"
            FROM (WITH t1 as
            (SELECT n.YYYY, n.Q, n.상권_구분명, n.상권명, n."count(*)" AS 업종수, n."sum(sales)" AS 총매출, n."sum(점포수)" AS 점포수, l.총_생활인구, w.총_직장인구, h.아파트_단지수, h.아파트_평균_시가 
            FROM sales_2 n 
            LEFT JOIN living l 
            ON n.YYYY=l.YYYY and n.Q=l.Q and n.상권명=l.상권명
            LEFT JOIN working w 
            ON n.YYYY=w.YYYY and n.Q=w.Q and n.상권명=w.상권명
            LEFT JOIN house h 
            ON n.YYYY=h.YYYY and n.Q=h.Q and n.상권명=h.상권명)
            SELECT *
            FROM t1) "source"
            WHERE ("source"."총_생활인구" IS NOT NULL
            AND "source"."총_직장인구" IS NOT NULL AND "source"."아파트_단지수" IS NOT NULL)
            ORDER BY "source"."총_직장인구" ASC;''')
        self.conn.commit()

    def get_columns(self):
        self.cur.execute('PRAGMA table_info(table_name);')
        print(self.cur.fetchall())

eda = db_eda()
eda.make_sales_2()
eda.make_join_table()

