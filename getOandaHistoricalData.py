import requests
from datetime import datetime, timedelta
import pandas as pd
import multiprocessing as mp
apiKey="MY_OANDA_API_KEY" # Get this from oanda

# This is the practice endpoint. If you want to use the real endpoint, change this to "https://api-fxtrade.oanda.com"
endpoint="https://api-fxpractice.oanda.com"

oandaAccountNumber = "100-000-00000000-001"
bearerToken = "ADD_SOME_TOKEN_HERE" #example: "Bearer oiun98dun29487n29472488338247j"
cookie = 'MIGHT_NEED_TO_ADD_COOKIE_HERE'

def getCandlesFromDates(tradingPair="USD_JPY", startDate="2022-12-01T00:00:00.000000000Z", endDate="2022-12-01T01:00:00.000000000Z"):
    import requests

    url = f"{endpoint}/v3/instruments/{tradingPair}/candles?from={startDate}&to={endDate}"

    payload={}
    headers = {
        'Accept-Datetime-Format': 'RFC3339',
        'Authorization': f"Bearer {apiKey}"
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    return response


def getCandlesFromCount(startDate="2022-12-01T00:00:00.000000000Z", count=5000, tradingPair="EUR_USD"): 
    import requests
    import json
    print(startDate)
    url = f"{endpoint}/v3/instruments/{tradingPair}/candles?from={startDate}&count={count}&granularity=M1"

    payload={}
    headers = {
        'Accept-Datetime-Format': 'RFC3339',
        'Authorization': f"Bearer {apiKey}"
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    # print(response.text)
    return json.loads(response.text)


def makeTrade():
    import requests
    import json
    url = f"https://api-fxpractice.oanda.com/v3/accounts/{oandaAccountNumber}/orders"


    payload = json.dumps({
    "order": {
        "price": 1.05418,
        "stopLossOnFill": {
        "timeInForce": "GTC",
        "price": 1.05428
        },
        "takeProfitOnFill": {
        "price": 1.05438
        },
        "timeInForce": "GTC",
        "instrument": "USD_JPY",
        "units": 1,
        "type": "LIMIT",
        "positionFill": "DEFAULT"
    }
    })
    headers = {
    'Content-Type': 'application/json',
    'Authorization': bearerToken,
    'Cookie': cookie
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    # print(response.text)



def getArrayOfDatetimesForNMinutes(n):
    now = datetime.now()
    last=0
    results=[]
    for x in range(0, n, 1000):
        results.append((now) - timedelta(minutes=(x-14121)))
    return results
    # return [now - timedelta(minutes=x) for x in range(0, n, 1000)]



# function that takes in number of days as parameter and returns array of dates before now in YYYY-MM-DD format
def getArrayOfDatesForNDays(n):
    n = int(n)
    now = datetime.now()

    return [now - timedelta(days=x) for x in range(0, n)]

# function that loops through array and turns datetime into YYYY-MM-DD format
def convertArrayOfDatetimesToDates(arrayOfDatetimes):
    return [x.strftime("%Y-%m-%d") for x in arrayOfDatetimes]

# calculates the number of days required to collect X 1m candles (change 14440 for total candles worth a day (for instance 5 minutes would be 1440 / 5))
def getNumberOfDaysForDefinedCandleCount(candlesRequested):
    return candlesRequested/1440

# function to check if key exists in dictionary
def keyExists(key, dictionary):
    if key in dictionary:
        return True
    else:
        return False

# get number of days required for X candles 
daysToGather = getNumberOfDaysForDefinedCandleCount(400000)

# returns array of dates from now to n days ago to gather all data in scope
arrayOfDates = getArrayOfDatesForNDays(daysToGather)

# converts array of datetimes to array of dates
arrayOfDates = convertArrayOfDatetimesToDates(arrayOfDates)

# setup the headers for how we will store the data 
historicalData = pd.DataFrame(columns =['time', 'open', 'high', 'low', 'close', 'volume'])


def handleData(result):
    data = result
    global historicalData

    for candle in data['candles']:
        if(keyExists('volume', candle)==True):
            volume = candle['volume']
        else:
            volume = 0
        open = candle['mid']['o']
        high = candle['mid']['h']
        low = candle['mid']['l']
        close = candle['mid']['c']
        time = candle['time']
       
        new_row = { 
            'time': str(time),
            'open': float(open), 
            'high': float(high), 
            'low': float(low),
            'close':float(close),
            'volume':int(volume)
        }
    
    # print('end')
        historicalCached = pd.concat([historicalData, pd.DataFrame([new_row])], ignore_index=True)

def handleError():
    print('error')

def main():
    i=0 
    for date in arrayOfDates:
        i+=1

        pool = mp.Pool(mp.cpu_count()-4)
        print(f"{i}/{len(arrayOfDates)}")
        dateToUse = f"{date}T00:00:00.000000000Z"

        pool.apply_async(getCandlesFromCount, args=(dateToUse, 5000), callback=handleData, error_callback=handleError)

    pool.close()
    pool.join()

    
if __name__ == "__main__":
    main()
    saveFileName = "oandaHistoricalData.csv"
    # export with 7 digit accuracy to handle all forex data pairs
    historicalData.to_csv(f"./{saveFileName}", index=False, float_format='%.7f')


