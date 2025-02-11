import pandas as pd
import requests
import logging
from loader import API_URL, db


def fetch_usd_to_uzs_rate():
    """
    Получает текущий курс USD к UZS через API.
    
    Отправляется запрос на API для получения текущих валютных курсов.
    Функция ищет курс USD и возвращает его значение.
    
    Возвращает:
        float: Курс USD к UZS.
        None: Если не удается получить данные или произошла ошибка.
    """
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        data = response.json()

        for item in data:
            if item['Ccy'] == 'USD':
                return float(item['Rate'])
    except requests.RequestException as e:
        logging.error(f"Ошибка при запросе API: {e}")
        return None

def save_exchange_rate_to_db(rate):
    """
    Сохранить курс валют в базу данных и закрыть соединение.
    """
    try:
        db.insert_exchange_rate(rate)
        logging.info(f"Курс валют USD -> UZS ({rate}) успешно сохранён в базу данных.")
    except Exception as e:
        logging.error(f"Ошибка при сохранении курса валют в базу данных: {e}")
    finally:
        if db:
            db.close()
            logging.info("Соединение с базой данных закрыто.")


def convert_to_usd(salary: str, date):
    """
    Конвертирует зарплату в USD на основе заданной валюты и курса на определенную дату.
    
    Разбирает строку с зарплатой, извлекает валюту и сумму, затем преобразует её в USD.
    Используется API для получения актуального курса валют на заданную дату.

    Аргументы:
        salary (str): Строка с зарплатой в формате 'от X до Y валюта'.
        date (datetime): Дата, на которую нужно получить курс валют.
    
    Возвращает:
        float: Зарплата в USD.
        None: Если не удается провести конвертацию.
    """
    if salary == 'empty':
        return None

    parts = salary.split(' ')
    num2 = int(parts[3])

    if num2 < 50:
        return None

    num1 = int(parts[1])
    currency = parts[4]
    avg_salary = (num1+num2)//2

    if currency == 'USD':
        return avg_salary

    url = f'https://cbu.uz/uz/arkhiv-kursov-valyut/json/all/{str(date).split(' ')[0]}'
    rest = pd.DataFrame(requests.get(url).json())
    rest['Rate'] = rest['Rate'].astype(float)

    if currency == 'UZS':
        if num2 > 50000:
            return avg_salary / rest[rest['Ccy']=='USD']['Rate'].values[0]
        else:
            return avg_salary
    else:
        return avg_salary * rest[rest['Ccy']==currency]['Rate'].values[0] / rest[rest['Ccy']=='USD']['Rate'].values[0]

def currency(message: dict):
    """
    Конвертирует зарплаты из различных валют в USD и добавляет это значение в сообщение.

    Эта функция вызывает `convert_to_usd` для конвертации зарплаты, затем сохраняет результат в поле 'salary_usd' 
    и возвращает обновлённое сообщение.
    
    Аргументы:
        message (dict): Словарь, содержащий данные о зарплате и дате.

    Возвращает:
        dict: Обновлённое сообщение с добавленным полем 'salary_usd'.
    """
    salary = message['salary']
    date = message['date']
    message['salary_usd'] = round(convert_to_usd(salary, date), 2)
    return message
