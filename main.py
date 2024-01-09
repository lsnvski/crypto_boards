import asyncio
import values
from InfluxDBDataProcessor import InfluxDBDataProcessor
import time

async def main():
    while True:
        tasks = []

        for pair in values.CRYPTO_PAIR:
            for timeframe in values.CRYPTO_TIMEFRAME:
                tasks.append(InfluxDBDataProcessor.create(values.EXCHANGE, pair, timeframe))

        # Wait for the initialization tasks to complete
        instances = await asyncio.gather(*tasks)

        # Now, perform subsequent tasks asynchronously for each initialized instance
        # Assuming you want to calculate indicators for each instance
        # calculation_tasks = [await x.calculate_indicators(indicators=['ADX', 'ADXR', 'APO']) for x in instances]

        calculation_tasks = []

        for x in instances:
            calculation_tasks.append(
                x.calculate_indicators(groups=values.GROUPS)
            
            )

        # Wait for the calculation tasks to complete
        await asyncio.gather(*calculation_tasks)

        # Finally, write to InfluxDB for each initialized instance
        write_tasks = [instance.write_df_influx() for instance in instances ]

        # Wait for the write tasks to complete
        await asyncio.gather(*write_tasks)
        time.sleep(60)
        
if __name__ == "__main__":
    asyncio.run(main())