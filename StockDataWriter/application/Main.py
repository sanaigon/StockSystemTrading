# -*- coding: utf-8 -*-

import pandas as pd

from repository.PostgresStockData import connect_db


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


def new_stock_dataframe(url):
    # 일자 데이터를 담을 df라는 dataframe 정의
    df = pd.DataFrame()

    for page in range(1, 20):
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


if __name__ == "__main__":
    item_name = '삼성전자'


    print(connect_db())

    code_df = get_stock_codes()

    code_df = reformat_code_dataframe(code_df)

    url = get_url(item_name, code_df)

    print(new_stock_dataframe(url).head())
