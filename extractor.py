import ccxt, pandas
import time, datetime
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
import values


## ===== Values =====
token = values.INFLUXDB_TOKEN
org = values.INFLUXDB_ORG
url = values.INFLUXDB_URL
bucket = values.INFLUXDB_BUCKET
crypto_symbol = values.CRYPTO_PAIR
crypto_timeframe = values.CRYPTO_TIMEFRAME

## Uncoment the following line to extract ohlcv data for particular date
# crypto_since = int(datetime.datetime.strptime(values.CRYPTO_SINCE, "%Y-%m-%d %H:%M:%S%z").timestamp() * 1000) 


## Initialize Binance ccxt and load markets
exchange = ccxt.binance()
exchange.enableRateLimit = True
exchange_markets = exchange.load_markets()

## Initialize influxdb client 
influx_client = InfluxDBClient(
    url=url,
    token=token,
    org=org,
    # debug=True
    )

## Initialize influxdb query api
query_api = influx_client.query_api()

## Get the last timestamp from influx
def latest_timestamp(pair, timeframe): 
    query = f'from(bucket: "{bucket}")\
        |> range(start: 0)\
        |> filter(fn: (r) => r["_measurement"] == "{pair}" and r.timeframe == "{timeframe}" and r._field == "_volume")\
        |> last()'
    
    result = query_api.query(org=org, query=query)
    last_time = list(result)[0].records[0].values["_time"]
    return int(datetime.datetime.strptime(str(last_time), "%Y-%m-%d %H:%M:%S%z").timestamp() * 1000) 
    

## GET and reform dataframe for infuxdb. Set timestamp as index
def ohlcv_to_df(crypto_pair, pair_timeframe):

    ohlcv_raw = exchange.fetch_ohlcv(
        symbol=crypto_pair,
        timeframe=pair_timeframe,
        since=latest_timestamp(pair=crypto_pair, timeframe=pair_timeframe),
        limit=1000
        )
    ohlcv_dataframe = pandas.DataFrame(
        ohlcv_raw,
        columns=[
            '_time',
            '_open',
            '_high',
            '_low',
            '_close',
            '_volume'
        ]
    )
    ohlcv_dataframe.set_index('_time', inplace=True)

    # Create data points
    data_points = [
        {
            "measurement": crypto_pair,
            "tags": {"timeframe": pair_timeframe},
            "time": timestamp,
            "fields": {field: float(value)}
        }
        for field, values in ohlcv_dataframe.items()
        for timestamp, value in values.items()
    ]

    yield data_points


write_client = influx_client.write_api(write_options=SYNCHRONOUS)

if len(crypto_timeframe) != 0 or 1 :
    while True:
        for p_timeframe in crypto_timeframe:
            for pair in crypto_symbol:
                point = ohlcv_to_df(pair, p_timeframe)
                write_client.write(
                    bucket=bucket,
                    record=point,
                    data_frame_measurement_name=crypto_symbol,
                    write_precision="ms"
                )
                print(pair + " - " + p_timeframe + "; Pulled")
        print("Sleeping 10s")
        time.sleep(10)
    
    else:
        # To do.
        pass
