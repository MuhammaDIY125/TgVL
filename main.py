from telethon import events
from loader import db
from myScript1 import client, channels, clean_text
from myScript2 import vacance_check
from myScript3 import parse_vacancy_details
from myScript4 import currency
from myScript5 import filter_position, filter_pl, filter_stack


@client.on(events.NewMessage(chats=channels))
async def handler(event):
    if event.chat:
        source = event.chat.username
        tg_id = event.message.id
        text = clean_text(event.message.message, source)
        date = event.message.date

        message = {
            'source': source,
            'tg_id': tg_id,
            'text': text,
            'date': date
        }

        if db.check_duplicate(
            source = message['source'],
            date = message['date'],
            text = message['text']
        ):
            return

        if vacance_check(message):
            return

        message = parse_vacancy_details(message)

        message = currency(message)

        message = filter_position(message)

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

        if company == 'empty':
            company = None

        if experience == 'empty':
            experience = 'Not Specified'

        if location == 'empty':
            location = None

        if position == 'empty':
            position = None


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

        db.insert_tg_data(main_id, source, tg_id, text, salary)

        for p in str(pl).split(', '):
            filter_id, correct_language = filter_pl(p)
            db.insert_pl(correct_language, main_id, filter_id)

        for s in str(stack).split(', '):
            filter_id, correct_stack = filter_stack(s)
            db.insert_stack(correct_stack, main_id, filter_id)

        print('Successful')

        db.close()

with client:
    client.run_until_disconnected()
