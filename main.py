import logging
from telethon import events
from loader import db
from myScript1 import client, channels, clean_text
from myScript2 import vacance_check
from myScript3 import parse_vacancy_details
from myScript4 import currency
from myScript5 import (
    filter_position,
    filter_pl,
    filter_stack,
    get_location_id,
    get_company_id,
    get_experience_id,
    get_source_id,
    main_filter
)


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
        if event.chat:
            message = {
                'source': event.chat.username,
                'tg_id': event.message.id,
                'text': clean_text(event.message.message, event.chat.username),
                'date': event.message.date
            }

            logging.info(f"Received message from {message['source']}, tg_id={message['tg_id']}")

            if db.check_duplicate(get_source_id(message['source']), message['date'], message['text']):
                logging.info("Duplicate message detected. Skipping...")
                return

            if vacance_check(message):
                logging.info("Message does not match vacancy criteria. Skipping...")
                return

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
            # salary = message['salary']
            pl = message['programming_language']
            stack = message['stack']
            salary_usd = str(message['salary_usd'])
            date = str(message['date'])
            tg_id = str(message['tg_id'])

            if company == 'empty':
                company = 'Not Specified'
            if experience == 'empty':
                experience = 'Not Specified'
            if location == 'empty':
                location = 'Not Specified'
            if position == 'empty':
                position = 'Other'


            location_id = get_location_id(location)
            company_id = get_company_id(company)
            experience_id = get_experience_id(experience)
            source_id = get_source_id(source)
            filter_id = main_filter(category, position)


            main_id = db.insert_main_vacancy(
                location_id=location_id,
                company_id=company_id,
                experience_id=experience_id,
                source_id=source_id,
                filter_id=filter_id,
                salary=salary_usd,
                date=date
            )
            logging.info(f"Inserted main vacancy with id: {main_id}")


            db.insert_tg_data(
                main_vacancy_id=main_id,
                tg_id=tg_id,
                text=text
            )
            logging.info("Inserted Telegram data.")


            if pl != 'empty':
                for p in str(pl).split(', '):
                    correct_id = filter_pl(p)
                    db.insert_pl(vacancy_id=main_id, correct_id=correct_id)
                logging.info("Inserted programming languages.")

            if stack != 'empty':
                for s in str(stack).split(', '):
                    correct_id = filter_stack(s)
                    db.insert_stack(vacancy_id=main_id, correct_id=correct_id)
                logging.info("Inserted tech stack.")


            logging.info("Handler executed successfully.")

    except Exception as e:
        logging.error(f"An error occurred in handler: {e}", exc_info=True)
    finally:
        db.close()


with client:
    client.run_until_disconnected()
