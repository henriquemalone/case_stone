#Popular banco de dados completo, desde a data 01-01-2021

import requests
import bigquerygoogle
import api
import time
from datetime import date, timedelta

#Cria tabela se nao existir
# bigquery.bq_create_table()

def fillDB():
    #Vetor com nome das moedas
    coins = ['bitcoin', 'ethereum']

    #Intervalo de dias entre dia 01-01-2021 e a data atual
    sdate = date(2021, 1, 1)   # data inicial
    edate = date.today()   # data atual

    delta = edate - sdate       # intervalo de dias

    #laço para percorrer todos os dias dentro do intervalo delta
    for i in range(delta.days + 1):
        day = sdate + timedelta(days=i)

        #Formato da data para dd-mm-aaaa
        cdate = '{}-{}-{}'.format(day.day, day.month, day.year)
        print(cdate)
        j = 0

        #laço para popular banco de dados
        for j in range(2):  
            data = api.dataFromApi(cdate, coins[j])

            id = data['id']
            symbol = data['symbol']
            name = data['name']
            usd = data['market_data']['current_price']['usd']
            eur = data['market_data']['current_price']['eur']
            brl = data['market_data']['current_price']['brl']

            # print(id, " | ", name, " | ", symbol, " | ", day, " | ", usd, " | ", eur," | ", brl)
            bigquerygoogle.insertDB(id, symbol, name, str(day), usd, eur, brl)
            time.sleep(1)


