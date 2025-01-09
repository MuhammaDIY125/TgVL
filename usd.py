import logging
import time
import schedule
from myScript4 import fetch_usd_to_uzs_rate, save_exchange_rate_to_db


if __name__ == "__main__":
    format = '%(filename)s - %(funcName)s - %(lineno)d - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(
        level=logging.INFO,
        format=format,
        filename='loggging.log',
    )


def update_currency_rate():
    try:
        rate = fetch_usd_to_uzs_rate()
        save_exchange_rate_to_db(rate)
        logging.info('Exchange rate saved successfully.')
    except Exception as e:
        logging.error(f'Error occurred: {e}')

schedule.every().day.at("00:10").do(update_currency_rate)

while True:
    schedule.run_pending()
    time.sleep(1)

# update_currency_rate()
