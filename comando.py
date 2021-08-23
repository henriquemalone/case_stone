# import requests
# from google.cloud import bigquery
from datetime import date, timedelta
import bigquerygoogle
import api
import popularbanco
import time

#Cria tabela se nao existir
if(bigquerygoogle.bq_create_table() == 'true'):
    #Popula tabela
    popularbanco.fillDB()
    print('Tabela criada e preenchida!')
else:
    
    #Vetor com nome das moedas
    coins = ['bitcoin', 'ethereum']

    #Intervalo de dias entre dia 01-01-2021 e a data atual
    sdate = date(2021, 1, 1)   # data inicial
    edate = date.today()   # data atual

    delta = edate - sdate       # intervalo de dias

    #laço para percorrer todos os dias dentro do intervalo delta
    if(bigquerygoogle.confDate(date.today()) == 'true'):
        print("Tabela ja atualizada")
    else:
        for i in range(2):  
            data = api.dataFromApi(date.today(), coins[i])

            id = data['id']
            symbol = data['symbol']
            name = data['name']
            usd = data['market_data']['current_price']['usd']
            eur = data['market_data']['current_price']['eur']
            brl = data['market_data']['current_price']['brl']

            # print(id, " | ", name, " | ", symbol, " | ", day, " | ", usd, " | ", eur," | ", brl)
            bigquerygoogle.insertDB(id, symbol, name, str(date.today()), usd, eur, brl)
            time.sleep(1)



