import requests
import json
from operator import itemgetter
import sqlite3

class api_to_db():
    def __init__(self):
        conn = sqlite3.connect('raw_data.db')
        self.conn = conn
        cur = conn.cursor()
        self.cur = cur
    

    def make_db (self, df, content, year):
        if df == 'sales':
            self.cur.execute(f'''create table if not exists {df}(
                YYYY INT,
                Q INT,
                상권_구분 VARCHAR(32),
                상권_구분명 VARCHAR(32),
                상권 INT,
                상권명  VARCHAR(32),
                업종_코드 VARCHAR(32),
                업종명 VARCHAR(32),
                sales float,
                점포수 INT
                );''')
            # print('sales table created')
            item = itemgetter('STDR_YY_CD', 'STDR_QU_CD', 'TRDAR_SE_CD', 'TRDAR_SE_CD_NM', 'TRDAR_CD','TRDAR_CD_NM', 'SVC_INDUTY_CD', 'SVC_INDUTY_CD_NM', 'THSMON_SELNG_AMT', 'STOR_CO')
            values = 'values(?,?,?,?,?,?,?,?,?,?)'
        elif df == 'living':
            self.cur.execute(f'''create table if not exists {df}(
                YYYY INT,
                Q INT,
                상권_구분 VARCHAR(32),
                상권_구분명 VARCHAR(32),
                상권 INT,
                상권명  VARCHAR(32),
                총_생활인구 INT,
                남성_생활인구 int,
                여성_생활인구 int
                );''')
            # print('living table created')
            item = itemgetter('STDR_YY_CD', 'STDR_QU_CD', 'TRDAR_SE_CD', 'TRDAR_SE_CD_NM', 'TRDAR_CD','TRDAR_CD_NM', 'TOT_FLPOP_CO', 'ML_FLPOP_CO', 'FML_FLPOP_CO')
            values = 'values(?,?,?,?,?,?,?,?,?)'
        elif df == 'working':
            self.cur.execute(f'''create table if not exists {df}(
                YYYY INT,
                Q INT,
                상권_구분 VARCHAR(32),
                상권_구분명 VARCHAR(32),
                상권 INT,
                상권명  VARCHAR(32),
                총_직장인구 INT,
                남성_직장인구 int,
                여성_직장인구 int
                );''')
            # print('working table created')
            item = itemgetter('STDR_YY_CD', 'STDR_QU_CD', 'TRDAR_SE_CD', 'TRDAR_SE_CD_NM', 'TRDAR_CD','TRDAR_CD_NM', 'TOT_WRC_POPLTN_CO', 'ML_WRC_POPLTN_CO', 'FML_WRC_POPLTN_CO')
            values = 'values(?,?,?,?,?,?,?,?,?)'
        elif df == 'house':
            self.cur.execute(f'''create table if not exists {df}(
                YYYY INT,
                Q INT,
                상권_구분 VARCHAR(32),
                상권_구분명 VARCHAR(32),
                상권 INT,
                상권명  VARCHAR(32),
                아파트_단지수 INT,
                아파트_평균_시가 int
                );''')
            # print('house table created')
            item = itemgetter('STDR_YY_CD', 'STDR_QU_CD', 'TRDAR_SE_CD', 'TRDAR_SE_CD_NM', 'TRDAR_CD','TRDAR_CD_NM', 'APT_HSMP_CO', 'AVRG_MKTC')
            values = 'values(?,?,?,?,?,?,?,?)'


        start = [k for k in range(1, 133000, 1000)]
        end = [k for k in range(1000, 133001, 1000)]

        for i, j in zip(start, end):
            try:
                API_URL = f"http://openapi.seoul.go.kr:8088/76485a52596d616e343153594f6e79/json/{content}/{i}/{j}/{year}"
                raw_data = requests.get(API_URL)
                df_raw = json.loads(raw_data.text)
                df_raw2 = df_raw[content]['row']
                length = len(df_raw2)

                for d in range(length):
                    # if d % 100 == 0:
                    # print(f'item: {item(df_raw2[d])}')
                    self.cur.execute(f'insert into {df} {values};', item(df_raw2[d]))
                    
                
                # print(f'{df} 테이블에 {i}부터 {j}까지 {length}개 레코드 삽입')
            except: 
                # print(f'---------{df}테이블에 {i}부터 {j}까지 못받음. 끝남 --------')
                break
        
        self.conn.commit()
        # print(f'commit done from {i} to {j}')


data = api_to_db()

table_list = {'sales':'VwsmTrdarSelngQq', 'living':'VwsmTrdarFlpopQq',
'working':'VwsmTrdarWrcPopltnQq', 'house':'InfoTrdarAptQq'}

for a, b in table_list.items():
    # print('------------------------------------------------------------')
    # print('\n[make 2021]')
    data.make_db(df=a, content=b, year=2021)
    # print('\n[make 2020]')
    data.make_db(df=a, content=b, year=2020)
    # print('----------------done-------------------')