import requests

def dataFromApi(date, coin):
    PARAMS = {'date': date}
    # sending get request and saving the response as response object
    r = requests.get(url = 'https://api.coingecko.com/api/v3/coins/'+coin+'/history?', params = PARAMS)

    # extracting data in json format
    data = r.json()

    return data
