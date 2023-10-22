from finpy.edgar.download import download
from finpy.utils.components import sp500
from finpy.utils.components import custom
from aiolimiter import AsyncLimiter
import asyncio
import time
import argparse
async def main(name, email, tickers):
    limiter = AsyncLimiter(1, 0.125)
    tasks = []
    semaphore = asyncio.Semaphore(value=10)
    r = {}
    for i in tickers:
        tasks.append(download.async_create(i, name, email, True, limiter, semaphore, r))
    await asyncio.wait(tasks)
    for i in r:
        print(i)
    return r
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Name and Email.')
    parser.add_argument('-name', help='Name')
    parser.add_argument('-email', help='E-Mail address')
    parser.add_argument('-tick', help='ticker file list')
    parser.add_argument('-sp500', action="store_true", default=False, help="include all tickers in s&p 500")
    args = parser.parse_args()
    tickers = custom(args.tick)
    if args.sp500:
        tickers += sp500();
        tickers = list(set(tickers))
    limiter = AsyncLimiter(1, 0.125)
    s = time.perf_counter()
    asyncio.run(main(args.name, args.email, tickers)) # Activate this line if the code is to be executed in VS Code
    # , etc. Otherwise deactivate it.
    # r = await main()          # Activate this line if the code is to be executed in Jupyter 
    # Notebook! Otherwise deactivate it.
    elapsed = time.perf_counter() - s
    print(f"Execution time: {elapsed:0.2f} seconds.")
