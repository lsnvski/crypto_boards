import ccxt.async_support as ccxt
import pandas as pd
import asyncio
import values
import datetime
from influxdb_client import Point
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
import talib  # Make sure to import talib library

class InfluxDBDataProcessor:

    def __init__(self, exchange, pair: list, timeframe: list):
        self.token = values.INFLUXDB_TOKEN
        self.org = values.INFLUXDB_ORG
        self.url = values.INFLUXDB_URL
        self.bucket = values.INFLUXDB_BUCKET
        self.crypto_symbol = values.CRYPTO_PAIR
        self.crypto_timeframe = values.CRYPTO_TIMEFRAME
        self.exchange = exchange
        self.pair = pair
        self.timeframe = timeframe
        self.df = None

    @classmethod
    async def create(cls, exchange, pair, timeframe):
        self = cls(exchange, pair, timeframe)
        await self.initialize_dataframe()  # Initialize dataframe asynchronously
        return self

    async def initialize_dataframe(self):
        self.dataframe = await self.ohlcv_to_df()

    async def query_influx_lasttimestamp(self):
        async with InfluxDBClientAsync(url=self.url, token=self.token, org=self.org) as influx_client:
            query = f'from(bucket: "{self.bucket}")\
                |> range(start: 0)\
                |> filter(fn: (r) => r["_measurement"] == "{self.pair}" and r.timeframe == "{self.timeframe}" and r._field == "_volume")\
                |> last()'

            query_api = influx_client.query_api()
            
            try:
                result = await query_api.query(query=query)
                last_time = list(result)[0].records[0].values["_time"]
                return int(datetime.datetime.strptime(str(last_time), "%Y-%m-%d %H:%M:%S%z").timestamp() * 1000)
            except Exception as e:
                return None

    async def ohlcv_to_df(self):
        try:
            exchange_class = getattr(ccxt, self.exchange)
            exchange_instance = exchange_class()
            exchange_instance.enableRateLimit = True

            last_timestamp = await self.query_influx_lasttimestamp()

            if last_timestamp is not None:
                response_raw = await exchange_instance.fetch_ohlcv(
                    symbol=self.pair,
                    timeframe=self.timeframe,
                    since=last_timestamp
                )
            else:
                response_raw = await exchange_instance.fetch_ohlcv(
                    symbol=self.pair,
                    timeframe=self.timeframe,
                    since=last_timestamp
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
            self.df = ohlcv_dataframe

            await exchange_instance.close()
            print(f"> ohlcv_to_df() successfully: -- {self.pair}_{self.timeframe}")

            self.df = ohlcv_dataframe

            return ohlcv_dataframe

        except Exception as e:
            print(f"> ohlcv_to_df() failed for: {self.pair}_{self.timeframe}\n\n > Error: {e}")

            await exchange_instance.close()
            return None

    async def query_influx_ohlcv(self):
        async with InfluxDBClientAsync(url=self.url, token=self.token, org=self.org) as influx_client:
            query = f'from(bucket: "{self.bucket}")\
                    |> range(start: 0)\
                    |> filter(fn: (r) => r["_measurement"] == "{self.pair}")\
                    |> filter(fn: (r) => r["timeframe"] == "{self.timeframe}")\
                    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'

            query_api_dataframe = influx_client.query_api()
            
            try:
                result_ohlcv_dataframe = await query_api_dataframe.query_data_frame(query)

                result_ohlcv_dataframe.set_index('_time', inplace=True)
                result_ohlcv_dataframe.index = pd.to_datetime(result_ohlcv_dataframe.index, unit='ms')
                relevant_columns = ['_measurement', 'timeframe', '_close', '_high', '_low', '_open', '_volume']
                result_ohlcv_dataframe = result_ohlcv_dataframe[relevant_columns]
                
                print(result_ohlcv_dataframe.tail(2))
                return result_ohlcv_dataframe
            
            except Exception as e:
                print(e)
                return None

    async def calculate_indicators(self):
        if self.df is not None:
            try:
                real = talib.OBV(self.df['_close'], self.df['_volume'])
                self.df['_real'] = real

                print(f"> calculate_indicators() successfully: -- {self.pair}_{self.timeframe}")
            except Exception as e:
                print(f"> calculate_indicators() failed for: {self.pair}_{self.timeframe}\n\n > Error: {e}")
                return None
        else:
            print(f"\nDataframe is empy or no Dataframe is defined: {self.df}")

    async def write_df_influx(self):
        async with InfluxDBClientAsync(self.url, self.token, self.org) as influx_client_write:
            write_api = influx_client_write.write_api()

            try:
                data_points = []

                # measurement, timeframe = self.df['_measurement'].iloc[0], self.df['timeframe'].iloc[0]

                for timestamp, values in self.df.iterrows():
                    point = Point(self.pair).tag("timeframe", self.timeframe).time(timestamp, write_precision='ms')
                    for field, value in values.items():
                        point.field(field, float(value))
                    data_points.append(point)

                await write_api.write(
                    bucket=self.bucket,
                    record=data_points,
                )
                print(f"> write_df_influx() successfully: -- {self.pair}_{self.timeframe}")

            except Exception as e:
                print(f"> write_df_influx() failed for: {self.pair}_{self.timeframe}\n > Error: {e}")

async def main():
    tasks = []

    for pair in values.CRYPTO_PAIR:
        for timeframe in values.CRYPTO_TIMEFRAME:
            tasks.append(InfluxDBDataProcessor.create('binance', pair, timeframe))

            # Wait for the initialization tasks to complete
    instances = await asyncio.gather(*tasks)

    # Now, perform subsequent tasks asynchronously for each initialized instance
    calculation_tasks = [x.calculate_indicators() for x in instances]

    # for instance in instances:
    #     # Assuming you want to calculate indicators for each instance
    #     calculation_tasks.append(instance.calculate_indicators())

   # Wait for the calculation tasks to complete
    await asyncio.gather(*calculation_tasks)

    # Perform other tasks if needed...

    # Finally, write to InfluxDB for each initialized instance
    write_tasks = []
    for instance in instances:
        write_tasks.append(instance.write_df_influx())

    # Wait for the write tasks to complete
    await asyncio.gather(*write_tasks)

if __name__ == "__main__":
    asyncio.run(main())
