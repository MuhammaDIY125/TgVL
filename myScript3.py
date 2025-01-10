import re
from loader import llm_chain_parse


locations = {
   'empty',
   'remote',
   'Tashkent city',
   'Republic of Karakalpakstan',
   'Andijan region',
   'Bukhara region',
   'Jizzakh region',
   'Kashkadarya region',
   'Navoi region',
   'Namangan region',
   'Samarkand region',
   'Surkhandarya region',
   'Syrdarya region',
   'Tashkent region',
   'Ferghana region',
   'Khorezm region'
}

experiences = {
   'empty',
   'No Experience',
   '1 Year <=',
   '1-3 Years',
   '3 Years >'
}

categories = {
    'FullStack',
    'Backend',
    'Frontend',
    'Mobile',
    'Game Development',
    'Design',
    'Marketing',
    'Management',
    'Q/A Testing',
    'Data Science',
    'DevOps',
    'Cybersecurity',
    'Robotics Engineering',
    'No Code',
    'Developer',
    'Other'
}


def extract_job_info(text):
    """
    Извлекает информацию о вакансии из текста сообщения.

    Аргументы:
    text (str): Текст сообщения, содержащий информацию о вакансии.

    Возвращает:
    str: Извлеченная информация о вакансии.
    """
    message = {"context": text}
    response = llm_chain_parse.invoke(message)
    return response.content.strip()

def modify_salary(salary: str):
    """
    Изменяет строку с информацией о зарплате, заменяя "empty" на найденное числовое значение.

    Аргументы:
    salary (str): Строка, содержащая информацию о зарплате.

    Возвращает:
    str: Обновленная строка зарплаты или 'empty', если числовое значение не найдено.
    """
    number = re.search(r'\d+', salary).group()
    if int(number):
        new_salary = salary.replace('empty', number)
        return new_salary
    else:
        return 'empty'

def parse_job_info(job_info):
    """
    Разбирает информацию о вакансии на отдельные компоненты.

    Аргументы:
    job_info (str): Строка с подробной информацией о вакансии.

    Возвращает:
    dict: Словарь с разобранной информацией о вакансии.
    """
    position = re.search(r"Position:\s*(.*)", job_info).group(1)
    experience = re.search(r"Experience:\s*(.*)", job_info).group(1)
    salary = re.search(r"Salary:\s*(.*)", job_info).group(1)
    location = re.search(r"Location:\s*(.*)", job_info).group(1)
    company = re.search(r"Company:\s*(.*)", job_info).group(1)
    stack = re.search(r"Stack:\s*(.*)", job_info).group(1)
    category = re.search(r"Category:\s*(.*)", job_info).group(1)
    programming_language = re.search(r"Programming language:\s*(.*)", job_info).group(1)

    if experience not in experiences:
        experience = 'empty'

    if ' empty ' in salary:
        salary = modify_salary(salary)

    if location not in locations:
        location = 'empty'

    if category not in categories:
        category = 'Other'

    return {
        'position': position,
        'experience': experience,
        'salary': salary,
        'location': location,
        'company': company,
        'stack': stack,
        'category': category,
        'programming_language': programming_language
    }

def parse_vacancy_details(message: dict):
    """
    Разбирает детали вакансии из текста сообщения и обновляет переданный словарь.

    Аргументы:
    message (dict): Словарь с ключами, включающими 'text'.

    Возвращает:
    dict: Обновленный словарь с подробной информацией о вакансии.
    """
    text = message['text']
    job_info = extract_job_info(text)
    parsed_data = parse_job_info(job_info)
    message.update(parsed_data)
    return message
