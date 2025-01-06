import re
import logging
from loader import db

def filter_position(message: dict):
    """
    Обрабатывает описание должности.
    Она применяет несколько замен с использованием регулярных выражений для стандартизации и очистки названия должности.
    
    Аргументы:
        message (dict): Словарь, содержащий должность под ключом 'position'.
    
    Возвращает:
        dict: Обновлённый словарь message с очищенной и стандартизированной должностью.
    """

    replacements = {
        r'\b(Full-Stack|Full Stack|Fullstack)\b': 'FullStack',
        r'\b(Professional|IT|Junior|Middle|Senior|Intern|Internship|Team Lead|Lead|Power BI|HTML|CSS|C, |Python, )\b': '',
        r'  ': ' ',
        r'^$': 'empty',
        r'\bNet\b': 'NET',
        r'\bNodeJS\b': 'Node.js',
        r'\b(Vue |VueJS)\b': 'Vue.js ',
        r'(React\.js|ReactJS|React Native|React native)': 'React',
        r'(React|Vue\.js|Node\.js|Next\.js|NestJS|Angular|TypeScript|//JavaScript)': 'JavaScript',
        r'\bJava/Kotlin Mobile\b': 'Kotlin Mobile',
        r'\b(Golang|Go Backend)\b': 'Go',
        r'\b(Flutter Mobile|Flutter)\b': 'Dart',
        r'(C# Backend|C# Xamarin|\.NET)': 'C#',
        r'(Java/Kotlin|Java Kotlin)': 'Java',
        r'\bJava Backend\b': 'Java',
        r'\b(GoPython|Python Odoo|Python Django)\b': 'Python',
        r'\b(PHP Laravel|PHP Yii2|Laravel|Yii2|Yii)\b': 'PHP',
        r'\b(SQL FullStack|SQL DBA|SQL Backend|PostgreSQL Backend|PostgreSQL|Oracle PL/SQL|Oracle|PLSQL|DBA)\b': 'SQL',
        r'\b(Swift Mobile|Swift|IOS)\b': 'iOS',
        r'\b(Mobile App|Mobile app)\b': 'Mobile',
        r'\b(Grafik dizayner|Grafik Designer|Graphic UI/UX Designer)\b': 'Graphic Designer',
        r'\bMoushn\b': 'Motion',
        r'\bQA\b': 'Q/A',
        r'\b(Videograf|Videograph)\b': 'Videographer',
        r'\b(Video Montador|Video Montager|Video Montaj Developer|Video Montaj Ustasi|Video Montajor|Video Montajyor|Videomantajor|Videomontajor|Videomontage Developer|Videomontajchi|Videomontajyor|Montajor|Video Montage Specialist|Video Editing Specialist|Video Editing Developer|Video Editer)\b': 'Video Editor',
        r'\b(UX Designer|UXUI Designer|UI Designer|UI/UX Developer|UX/UI Designer|Product Designer|Web Designer|Mobile App Designer|Layout Designer|UXUI|UI/UX|UX UI|UI/UX Designer Developer)\b': 'UI/UX Designer',
        r'\bUI/UX Designer designer\b': 'UI/UX Designer',
        r'Mobilograph Developer': 'Mobilograph',
        r'\b FullStack Developer\b': ' FullStack',
        r'\b Frontend Developer\b': ' Frontend',
        r'\b Backend Developer\b': ' Backend',
        r'\b Mobile Developer\b': ' Mobile',
        r'\b Android Developer\b': ' Android',
    }

    text = message['position']

    for pattern, replacement in replacements.items():
        text = re.sub(pattern, replacement, text)

    text = re.sub(r'\b(\w+)(?:\s*/\s*\1)+\b', r'\1', text)  # Убирает повторы с разделителем "/"
    text = re.sub(r'\b(\w+)(?:\s+\1)+\b', r'\1', text)      # Убирает повторы слов через пробел

    text = text.strip()

    if text in ['Java', 'Go', 'Dart', 'JavaScript', 'Python', 'PHP', 'SQL', 'C#', 'FullStack', 'Frontend', 'Backend', 'Mobile', 'Android', 'iOS', 'AI']:
        text = f'{text} Developer'
    elif text == '':
        text = 'empty'

    message['position'] = text.strip()

    return message


def filter_pl(pl):
    """
    Проверяет, существует ли даннай язык программирования в датабазе.
    Если язык программирования существует, возвращаются её ID и корректное значение.
    Если нет, строка добавляется в таблицу как 'new'.
    
    Аргументы:
        pl (str): Описание должности или роли для проверки в таблице фильтрации.
    
    Возвращает:
        tuple: ID и корректное значение (либо уже существующее, либо 'new').
    """
    try:
        res = db.filter_pl(pl)
        if res:
            return res.id, res.correct
        else:
            filter_id = db.insert_into_filtered_pl(incorrect=pl, correct='new')
            return filter_id, 'new'
    except Exception as err:
        db.session.rollback()
        logging.error(err)

def filter_stack(stack):
    """
    Проверяет, существует ли даннай стэк в датабазе.
    Если стэк существует, возвращаются её ID и корректное значение.
    Если нет, строка добавляется в таблицу как 'new'.
    
    Аргументы:
        stack (str): Технологический стек для проверки в таблице фильтрации.
    
    Возвращает:
        tuple: ID и корректное значение (либо уже существующее, либо 'new').
    """
    try:
        res = db.filter_stack(stack)
        if res:
            return res.id, res.correct
        else:
            filter_id = db.insert_into_filtered_stack(incorrect=stack, correct='new')
            return filter_id, 'new'
    except Exception as err:
        db.session.rollback()
        logging.error(err)
