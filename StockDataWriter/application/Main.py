# -*- coding: utf-8 -*-
from urllib.request import urlopen

import pandas as pd
from bs4 import BeautifulSoup
from StockDataWriter.repository.PostgresStockData import PostgresStockData


def get_stock_codes():
    """

    종목 코드를 dataframe의 형태로 얻기위한 함수
    :return: Code 정보가 담긴 dataframe의 리스트 반환
    """
    return pd.read_html('http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13', header=0)[0]


def get_url(item_name, code_df):
    """
    naver로 부터 해당 code 정보로 부터 주식정보를 얻기

    :param item_name: 종목명
    :param code_df: code dataframe
    :return: 종목명으로 검색된 code와의 조합된 URL
    """
    code = code_df.query("name=='{}'".format(item_name))['code'].to_string(index=False)
    url = 'http://finance.naver.com/item/sise_day.nhn?code={code}'.format(code=code)
    return url


def reformat_code_dataframe(code_df):
    """
    code data frame을 reforamt하기 위한 함수

    :param code_df: 상장code가 담긴 dataframe
    :return: 수정된 dataframe
    """
    # 필요없는 컬럼들을 제거
    code_df = code_df[['회사명', '종목코드']]

    # 한글로된 컬럼명을 영어로 변경
    code_df = code_df.rename(columns={'회사명': 'name', '종목코드': 'code'})

    # 코드를 6자리로 변경
    code_df.code = code_df.code.map('{:06d}'.format)
    return code_df


def new_stock_dataframe(url, max_page_num):
    '''
    url로 부터 종목정보를 담은 dataframe을 만든다.
    dataframe으로 담은 후 날짜별로 오름차순 정렬한다.

    :param url: 종목정보가 담긴 url
    :param max_page_num: max_page_num 정보
    :return: 종목정보가 담긴 dataframe
    '''
    # 일자 데이터를 담을 df라는 dataframe 정의
    df = pd.DataFrame()

    for page in range(1, max_page_num):
        page_url = '{url}&page={page}'.format(url=url, page=page)
        df = df.append(pd.read_html(page_url, header=0)[0], ignore_index=True)

    # 결측값 있는 행 제거
    df = df.dropna()

    # 한글로 된 컬럼명을 영어로 바꿔줌
    df = df.rename(columns={'날짜': 'date',
                            '종가': 'close',
                            '전일비': 'diff',
                            '시가': 'open',
                            '고가': 'high',
                            '저가': 'low',
                            '거래량': 'volume'})

    df[['close', 'diff', 'open', 'high', 'low', 'volume']] \
        = df[['close', 'diff', 'open', 'high', 'low', 'volume']].astype(int)

    # date의 타입을 날짜로 변경
    df['date'] = pd.to_datetime(df['date'])

    # date 기준으로 dataframe을 오름차순 정렬
    df = df.sort_values(by=['date'], ascending=True)

    return df


def max_page_num(url):
    """
    각 url의 max page를 가져오는 함수

    :param url: 시세 페이지가 표시된 url
    :return: max_page_num
    """
    html = urlopen(url)
    source = BeautifulSoup(html.read(), "html.parser")
    max_page = source.find_all("table", align="center")
    mp = max_page[0].find_all("td", {"class": "pgRR"})
    mp_num = int(mp[0].a.get('href').split('&')[1].split('=')[1])
    return mp_num + 1


if __name__ == "__main__":
    pg = PostgresStockData()
    pg.connect_db("localhost", "postgres", "jkpark")

    code_df = get_stock_codes()

    ## 회사명과 종목코드만 dataframe으로 만든다
    code_df = reformat_code_dataframe(code_df)

    # (종목코드, 종목정보 url)을 담을 리스트
    stock_info_list = []

    # 종목코드를 이용해 데이터를 가져올 url을 지정한다.
    for row in code_df.iterrows():
        item_name = row[1]['name']
        stock_info_list.append((row[1]['code'], get_url(item_name, code_df)))

    for stock_info in stock_info_list:
        code = stock_info[0]
        url = stock_info[1]
        max_num = max_page_num()
        print("insert data that be got from: {0}, max_num={1}".format(url, max_num))
        # url에서 부터 종목별 데이터가 담긴 dataframe을 얻은 후 db에 저장한다
        for _, row in new_stock_dataframe(url, max_num).iterrows():
            pg.save(code,
                    row['date'],
                    str(row['open']),
                    str(row['high']),
                    str(row['low']),
                    str(row['close']),
                    str(row['diff']),
                    str(row['volume']))
