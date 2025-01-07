import logging
from telethon import events
from loader import db
from myScript1 import client, channels, clean_text
from myScript2 import vacance_check
from myScript3 import parse_vacancy_details
from myScript4 import currency
from myScript5 import filter_position, filter_pl, filter_stack


if __name__ == "__main__":
    format = '%(filename)s - %(funcName)s - %(lineno)d - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(
        level=logging.INFO,
        format=format,
        filename='loggging.log',
    )


@client.on(events.NewMessage(chats=channels))
async def handler(event):
    try:
        # Логируем начало обработки события
        logging.info("Handler triggered with new message.")

        if event.chat:
            source = event.chat.username
            tg_id = event.message.id
            text = clean_text(event.message.message, source)
            date = event.message.date

            logging.info(f"Received message from {source}, tg_id={tg_id}, date={date}")

            message = {
                'source': source,
                'tg_id': tg_id,
                'text': text,
                'date': date
            }

            # Проверка дубликатов
            if db.check_duplicate(
                source=message['source'],
                date=message['date'],
                text=message['text']
            ):
                logging.info("Duplicate message detected. Skipping...")
                return

            # Проверка на соответствие вакансии
            if vacance_check(message):
                logging.info("Message does not match vacancy criteria. Skipping...")
                return

            # Логируем детали сообщения
            message = parse_vacancy_details(message)
            logging.info(f"Parsed message details: {message}")

            message = currency(message)
            logging.info("Converted currency.")

            message = filter_position(message)
            logging.info("Filtered position.")

            category = message['category']
            position = message['position']
            location = message['location']
            experience = message['experience']
            company = message['company']
            source = message['source']
            text = message['text']
            salary = message['salary']
            pl = message['programming_language']
            stack = message['stack']
            salary_usd = str(message['salary_usd'])
            date = str(message['date'])
            tg_id = str(message['tg_id'])

            # Обработка пустых значений
            if company == 'empty':
                company = None
            if experience == 'empty':
                experience = 'Not Specified'
            if location == 'empty':
                location = None
            if position == 'empty':
                position = None

            # Вставка в базу данных
            main_id = db.insert_main_vacancy(
                category,
                position,
                location,
                experience,
                company,
                salary_usd,
                source,
                date
            )
            logging.info(f"Inserted main vacancy with id: {main_id}")

            db.insert_tg_data(main_id, source, tg_id, text, salary)
            logging.info("Inserted Telegram data.")

            # Обработка и запись языков программирования
            for p in str(pl).split(', '):
                filter_id, correct_language = filter_pl(p)
                db.insert_pl(correct_language, main_id, filter_id)
            logging.info("Inserted programming languages.")

            # Обработка и запись стека технологий
            for s in str(stack).split(', '):
                filter_id, correct_stack = filter_stack(s)
                db.insert_stack(correct_stack, main_id, filter_id)
            logging.info("Inserted tech stack.")

            logging.info("Handler executed successfully.")

    except Exception as e:
        # Логирование ошибок
        logging.error(f"An error occurred in handler: {e}", exc_info=True)
    finally:
        db.close()


with client:
    client.run_until_disconnected()
