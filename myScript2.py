import logging
from langchain.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from loader import OPENAI_API_KEY


prompt_template = ChatPromptTemplate.from_template(
"""
Ты классификатор сообщений. Тебе будут встречаться резюме, вакансии, реклама и другие типы сообщений.
Твоя задача:
1. Если сообщение — вакансия, связанная с IT, напиши "1".
2. Если сообщение — вакансия, но не связана с IT, напиши "0".
3. Если сообщение не является вакансией, напиши "0".

Как определить вакансию:
- Сообщение описывает компанию (рассказывает о себе).
- В тексте говорится, что они кого-то ищут.
- Используются фразы во множественном числе (например, "мы ищем").
- Приведены требования к кандидату.

IT-профессии включают:
- Программистов (любой язык и профессии, связанные с программированием).
- DevOps.
- Проектных и продуктовых менеджеров.
- Дизайнеров: UI/UX, Motion, 3D, графических.
- SMM-специалистов и аналогичные профессии.
- Мобилографов, видеографов, видеомонтажёров.

Не входят в IT:
- Менеджеры продаж, бренд-менеджеры, менеджеры Call-центров.
- Учителя и менторы (даже если обучают программированию).
- Call-операторы.
- Администраторы.
- HR.
- Ассистенты.
- Brand face.

Текст сообщения: {context}
"""
)

llm = ChatOpenAI(model="gpt-4o-mini", api_key=OPENAI_API_KEY)

llm_chain = prompt_template | llm

def classify_message(text):
    """
    Классифицирует текст сообщения на основе указанных правил.

    Аргументы:
    text (str): Текст сообщения для классификации.

    Возвращает:
    str: "1", если сообщение — IT-вакансия; "0" в остальных случаях.
    """
    response = llm_chain.invoke({"context": text})
    result = response.content.strip()
    return result

def vacance_check(message :dict):
    """
    Проверяет, является ли сообщение IT-вакансией.

    Аргументы:
    message (dict): Словарь с данными сообщения, где ключи включают 'source' и 'text'.

    Возвращает:
    bool или None: True, если сообщение — IT-вакансия; None, если оно не подпадает под критерии.
    """

    source = message['source']
    text = str(message['text'])

    """
    В канале UstozShogird все вакансии начинаются с загаловка 'Xodim kerak:'. 
    Благодаря этому мы можем заранее определить подходит ли нам это сообщение или нет.
    Но даже в вакансих есть неподходящие нам позиции, к примеру менеджер продаж, поэтому этот код уберает лишь 100% неподходящие нам сообщения.
    """
    if source == 'UstozShogird' and not text.startswith('Xodim kerak:'):
        logging.info(f"Message from '{source}' skipped as it does not start with 'Xodim kerak:'. Text: {text}")
        return None

    if classify_message(text) == '1':
        return True
