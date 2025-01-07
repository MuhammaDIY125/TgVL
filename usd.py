import time
import schedule
from myScript4 import fetch_usd_to_uzs_rate, save_exchange_rate_to_db


def update_currency_rate():
    rate = fetch_usd_to_uzs_rate()
    save_exchange_rate_to_db(rate)

# Запуск задачи каждый день в 00:10
schedule.every().day.at("00:10").do(update_currency_rate)

while True:
    schedule.run_pending()
    time.sleep(1)
