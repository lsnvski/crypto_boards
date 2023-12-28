import ccxt.async_support as ccxt
import pandas as pd
import asyncio
import time, datetime
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
import values


## ===== Values =====
token = values.INFLUXDB_TOKEN
org = values.INFLUXDB_ORG
url = values.INFLUXDB_URL
bucket = values.INFLUXDB_BUCKET
crypto_symbol = values.CRYPTO_PAIR
crypto_timeframe = values.CRYPTO_TIMEFRAME

## Initialize influxdb client 
async def query_influx_lasttimestamp(pair, timeframe):
    async with InfluxDBClientAsync(url=url, token=token, org=org,) as influx_client:

        query = f'from(bucket: "{bucket}")\
            |> range(start: 0)\
            |> filter(fn: (r) => r["_measurement"] == "{pair}" and r.timeframe == "{timeframe}" and r._field == "_volume")\
            |> last()'

        ## Initialize influxdb query api
        query_api = influx_client.query_api()
        
        try:
            result = await query_api.query(query=query)
            # print(list(result)[0].records[0].values["_time"])

            last_time = list(result)[0].records[0].values["_time"]
            return(int(datetime.datetime.strptime(str(last_time), "%Y-%m-%d %H:%M:%S%z").timestamp() * 1000))
        
        except Exception as e:
            return None
    
# ohlcv_last_timestamp = asyncio.run(query_influx_lasttimestamp("BTC/USDT", "4h"))
# print(ohlcv_last_timestamp)

# GET and reform dataframe for infuxdb. Set timestamp as index
async def ohlcv_to_df(exchange, crypto_pair, pair_timeframe):

    # await exchange_instance.load_markets()

    print(f"\n-------\nExtract OHLCV from\nExchange: {exchange}, \nPair: {crypto_pair}, {crypto_timeframe} \n-------\n\n")
    try:
        
        ## Initialize Binance ccxt and load markets
        exchange_class = getattr(ccxt, exchange)
        exchange_instance = exchange_class()
        exchange_instance.enableRateLimit = True
        # await exchange_instance.load_markets()

        last_timestamp = await query_influx_lasttimestamp(pair=crypto_pair, timeframe=pair_timeframe)

        if last_timestamp != None:
            response_raw = await exchange_instance.fetch_ohlcv(
                symbol=crypto_pair,
                timeframe=pair_timeframe,
                since= await query_influx_lasttimestamp(pair=crypto_pair, timeframe=pair_timeframe)
                )
        else:
            response_raw = await exchange_instance.fetch_ohlcv(
                symbol=crypto_pair,
                timeframe=pair_timeframe,
                since= await query_influx_lasttimestamp(pair=crypto_pair, timeframe=pair_timeframe)
                )

        
        ohlcv_dataframe = pd.DataFrame(
            response_raw,
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
        ohlcv_dataframe.index = pd.to_datetime(ohlcv_dataframe.index, unit='ms')
        # print(f" > successfully: \nType: {type(ohlcv_dataframe)}, \nLen: {(len(ohlcv_dataframe))}") 
        # print(ohlcv_dataframe.tail(10))
        await exchange_instance.close()

    except Exception as e:
        print(e)
        print('fetch_OHLCV() failed')
        await exchange_instance.close()

    async with InfluxDBClientAsync(url, token, org) as influx_client_write:

        write_api = influx_client_write.write_api()

        print(f"\n------- Write data by async API: -------\n")
        try:
           # Create data points as list
            data_points = []
            
            # point = Point(crypto_pair).tag("_timeframe", pair_timeframe).time(timestamp, write_precision='ms')

            # for field, value in ohlcv_dataframe.items():
            #     point.field(field=field, value=value)
            #     print(f"\n\nField: {field}\nType: {type(field)}\nValue: {value}\nType: {type(value)}")

            for timestamp, values in ohlcv_dataframe.iterrows():
                point = Point(crypto_pair).tag("timeframe", pair_timeframe).time(timestamp, write_precision='ms')

                for field, value in values.items():
                    point.field(field, float(value))
                
                data_points.append(point)
            
            
            # print(data_points)

            successfully = await write_api.write(
                bucket = bucket, 
                record = data_points,
                data_frame_measurement_name = crypto_pair,
                # data_frame_timestamp_column = ohlcv_dataframe.index.name,
                # data_frame_tag_columuns = pair_timeframe
                )
            print(f" > successfully: {successfully}")

        except Exception as e:
            print(e)

async def main():

    # List of asynchronous functions to run concurrently
    tasks = []

    for pair in crypto_symbol:
        for timeframe in crypto_timeframe:
            tasks.append(ohlcv_to_df("binance", pair, timeframe))

    # Run the tasks concurrently
    await asyncio.gather(*tasks)

# asyncio.run(ohlcv_to_df(binance, 'ADA/USDT', '1h'))
if __name__ == "__main__":
    asyncio.run(main())
