import ccxt.async_support as ccxt
import pandas as pd
import values, asyncio, inspect
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
        """
        Asynchronous function to query the InfluxDB database and retrieve the timestamp of the latest data point.

        Returns:
            int: Timestamp of the latest data point in milliseconds since epoch, or None if an error occurs.
        """

        # Establish asynchronous connection to InfluxDB
        async with InfluxDBClientAsync(url=self.url, token=self.token, org=self.org) as influx_client:
            
            # Construct InfluxDB Flux query
            query = f'from(bucket: "{self.bucket}")\
                    |> range(start: 0)\
                    |> filter(fn: (r) => r["_measurement"] == "{self.pair}" and r.timeframe == "{self.timeframe}" and r._field == "_volume")\
                    |> last()'

            # Get InfluxDB query API
            query_api = influx_client.query_api()

            try:
                # Execute the asynchronous query
                result = await query_api.query(query=query)

                # Extract and process the timestamp of the latest data point
                last_time = list(result)[0].records[0].values["_time"]
                return int(datetime.datetime.strptime(str(last_time), "%Y-%m-%d %H:%M:%S%z").timestamp() * 1000)
            
            except Exception as e:
                # Handle exceptions and return None in case of an error
                return None

    async def ohlcv_to_df(self):
        """
        Asynchronous function to fetch OHLCV data from a cryptocurrency exchange and convert it into a DataFrame.

        Returns:
            pd.DataFrame: DataFrame containing OHLCV data, or None if an error occurs.
        """
        try:
            # Dynamically create an instance of the specified exchange class using ccxt
            exchange_class = getattr(ccxt, self.exchange)
            exchange_instance = exchange_class()
            exchange_instance.enableRateLimit = True

            # Get the timestamp of the latest data point from InfluxDB
            last_timestamp = await self.query_influx_lasttimestamp()

            # # Fetch OHLCV data from the exchange
            # if last_timestamp is not None:
            #     response_raw = await exchange_instance.fetch_ohlcv(
            #         symbol=self.pair,
            #         timeframe=self.timeframe,
            #         since=last_timestamp
            #     )
            # else:
            #     response_raw = await exchange_instance.fetch_ohlcv(
            #         symbol=self.pair,
            #         timeframe=self.timeframe,
            #     )
            response_raw = await exchange_instance.fetch_ohlcv(
                symbol=self.pair,
                timeframe=self.timeframe,
            )

            # Convert the raw response to a Pandas DataFrame
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

            # Set the '_time' column as the DataFrame index and convert it to datetime format
            ohlcv_dataframe.set_index('_time', inplace=True)
            ohlcv_dataframe.index = pd.to_datetime(ohlcv_dataframe.index, unit='ms')

            # Update the class attribute with the resulting DataFrame
            self.df = ohlcv_dataframe

            # Close the exchange connection
            await exchange_instance.close()

            # Print success message and return the DataFrame
            print(f"> ohlcv_to_df() successfully: -- {self.pair}_{self.timeframe}")
            return ohlcv_dataframe

        except Exception as e:
            # Handle exceptions and return None in case of an error
            print(f"> ohlcv_to_df() error: {str(e)}")
            
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

    async def calculate_indicators(self, groups: list = None, indicators: list = None, **kwargs):
        """
        Asynchronous function to calculate TA-Lib indicators based on the provided groups or a specific list of indicators.

        Parameters:
            groups (list): List of indicator groups to calculate. Default is None.
            indicators (list): List of specific indicator names to calculate. Default is None.
            **kwargs: Additional keyword arguments for indicator parameters.

        Returns:
            None: The function updates the DataFrame with calculated indicators.
        """

        if self.df is not None:
            all_indicators = talib.get_function_groups()

            if groups:
                indicators_to_calculate = [indicator for group in groups for indicator in all_indicators[group]]

            elif indicators:
                indicators_to_calculate = indicators

            else:
                print("No groups or indicators specified.")
                return None

            for indicator in indicators_to_calculate:
                try:
                    # Use the DataFrame columns as arguments
                    arguments = self.df.columns.tolist()

                    # Remove the time column if it exists
                    # arguments.remove('_measurement')
                    # arguments.remove('timeframe')

                    # Use additional keyword arguments for specific parameters
                    if indicator in kwargs:
                        additional_args = kwargs[indicator]
                        arguments.extend(additional_args)

                    # Initiate the indicator class & get arguments 
                    indicator_instance = getattr(talib, indicator)
                    indicator_signature = inspect.signature(indicator_instance)

                    param_names = []
                    for param in indicator_signature.parameters.values():
                        if param.name == 'real':
                            param_names.append('_close')
                        elif f'_{param.name}' in arguments:
                            param_names.append(f'_{param.name}')
                    
                    # if len(param_names) == 1:
                    #     indicator_instance_init = indicator_instance(self.df[param_names[0]])
                    # else:
                    #     indicator_instance_init = indicator_instance(*[self.df[param] for param in param_names])

                    indicator_instance_init = indicator_instance(*[self.df[param] for param in param_names])

                    # Create a DataFrame with the new columns
                    result_df = pd.DataFrame(index=self.df.index)
                    df_indicator_column = f'_{indicator.lower()}'

                    if isinstance(indicator_instance_init, pd.Series):
                        indicator_instance_init.name = df_indicator_column
                        result_df[df_indicator_column] = indicator_instance_init

                    elif isinstance(indicator_instance_init, tuple):
                        for num, series in enumerate(indicator_instance_init):
                            series.name = df_indicator_column + f'-{num}'
                            result_df[series.name] = series

                    # Concatenate the new DataFrame with the original DataFrame
                    self.df = pd.concat([self.df, result_df], axis=1)

                    print(f"> calculate_indicators();{indicator} successfully: -- {self.pair}_{self.timeframe}")

                except Exception as e:
                    print(f"> calculate_indicators();{indicator} failed for: {self.pair}_{self.timeframe}\n\n > Error: {e}")
                    # print(f"Length of result: {len(result)}\nResult: {result}")
                    # print(f"Number of rows in DataFrame: {self.df.tail(10)}")
                    return None
        else:
            print(f"\nDataframe is empty or no Dataframe is defined: {self.df}")

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

# Example calls
# await instances.calculate_indicators(groups=['Momentum Indicators'])
# or

# instance = asyncio.run(InfluxDBDataProcessor.create(values.EXCHANGE, "BTC/USDT", '4h'))

# calculators = asyncio.run(instance.calculate_indicators(groups=values.GROUPS)) 

# # calculators = asyncio.run(instance.calculate_indicators(indicators=["BBANDS"])) 
# asyncio.run(instance.write_df_influx())
 