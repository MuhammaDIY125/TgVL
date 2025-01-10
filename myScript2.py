import logging
from loader import llm_chain_classify


def classify_message(text):
    """
    Классифицирует текст сообщения на основе указанных правил.

    Аргументы:
    text (str): Текст сообщения для классификации.

    Возвращает:
    str: "1", если сообщение — IT-вакансия; "0" в остальных случаях.
    """
    response = llm_chain_classify.invoke({"context": text})
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

    if source == 'UstozShogird' and not text.startswith('Xodim kerak:'):
        logging.info(f"Message from '{source}' skipped as it does not start with 'Xodim kerak:'. Text: {text}")
        return None

    if classify_message(text) == '1':
        return True
