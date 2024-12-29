from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
import logging
from datetime import datetime, timedelta


Base = declarative_base()

class MainVacancy(Base):
    """
    Таблица для хранения основной информации о вакансиях.
    """
    __tablename__ = 'main_vacancy'
    id = Column(Integer, primary_key=True)
    category = Column(String)
    position = Column(String)
    location = Column(String)
    experience = Column(String)
    company = Column(String)
    salary = Column(Float)
    source = Column(String)
    date = Column(DateTime)

    tg_data = relationship('TGData', back_populates='main_vacancy')

class TGData(Base):
    """
    Таблица для хранения данных о вакансиях из Telegram.
    """
    __tablename__ = 'tg_data'
    vacancy_id = Column(Integer, primary_key=True)
    main_vacancy_id = Column(Integer, ForeignKey('main_vacancy.id'))
    source = Column(String)
    tg_id = Column(String)
    text = Column(String)
    salary = Column(Float)

    main_vacancy = relationship('MainVacancy', back_populates='tg_data')

class ExchangeRate(Base):
    """
    Таблица для хранения информации о курсе валют.
    """
    __tablename__ = 'exchange_rates'
    id = Column(Integer, primary_key=True)
    rate = Column(Float)
    timestamp = Column(DateTime)

class ProgrammingLanguage(Base):
    """
    Таблица для хранения информации о языках программирования.
    """
    __tablename__ = 'programming_language'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    vacancy_id = Column(Integer)
    corrections_id = Column(Integer)

class Stack(Base):
    """
    Таблица для хранения информации о стеке технологий.
    """
    __tablename__ = 'stack'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    vacancy_id = Column(Integer)
    corrections_id = Column(Integer)

class LanguageCorrection(Base):
    """
    Таблица для хранения исправлений названий языков программирования.
    """
    __tablename__ = 'language_corrections'
    id = Column(Integer, primary_key=True)
    correct = Column(String)
    incorrect = Column(String)

class StackCorrection(Base):
    """
    Таблица для хранения исправлений названий стеков технологий.
    """
    __tablename__ = 'stack_corrections'
    id = Column(Integer, primary_key=True)
    correct = Column(String)
    incorrect = Column(String)


class Database:
    def __init__(self, host, user, password, database):
        """
        Инициализация подключения к базе данных.

        :param host: Хост базы данных
        :param user: Имя пользователя
        :param password: Пароль пользователя
        :param database: Название базы данных
        """
        self.engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{database}')
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()


    def insert_exchange_rate(self, rate):
        """
        Обновляет курс валюты в базе данных.

        :param rate: Новый курс валюты
        """
        try:
            self.session.query(ExchangeRate).delete()
            exchange_rate = ExchangeRate(rate=rate, timestamp=datetime.now())
            self.session.add(exchange_rate)
            self.session.commit()
            print(f"Обновлено: новый курс валюты {rate} добавлен.")
        except Exception as e:
            self.session.rollback()
            logging.error(f"Ошибка при обновлении курса: {e}")


    def check_vacancy_exists(self, source, tg_id):
        """
        Проверяет, существует ли вакансия с указанным источником и Telegram ID.

        :param source: Источник вакансии
        :param tg_id: Telegram ID вакансии
        :return: True, если вакансия существует, иначе False
        """
        return self.session.query(TGData).filter_by(source=source, tg_id=tg_id).first() is not None

    def check_duplicate(self, source, date, text):
        """
        Проверяет наличие дубликата текста вакансии за последние 30 дней.

        :param source: Источник вакансии
        :param date: Дата вакансии
        :param text: Текст вакансии
        :return: True, если текст уже существует, иначе False
        """
        date_lower_bound = date - timedelta(days=30)
        filtered_texts = self.session.query(TGData.text).join(MainVacancy).filter(
            MainVacancy.date.between(date_lower_bound, date),
            MainVacancy.source == source
        ).all()

        filtered_texts = [item[0] for item in filtered_texts]
        return text in filtered_texts


    def insert_main_vacancy(self, category, position, location, experience, company, salary, source, date):
        """
        Добавляет основную вакансию в базу данных.

        :param category: Категория вакансии
        :param position: Должность
        :param location: Местоположение
        :param experience: Требуемый опыт
        :param company: Компания
        :param salary: Зарплата
        :param source: Источник вакансии
        :param date: Дата вакансии
        :return: ID добавленной вакансии
        """
        vacancy = MainVacancy(
            category=category,
            position=position,
            location=location,
            experience=experience,
            company=company,
            salary=salary,
            source=source,
            date=date
        )
        self.session.add(vacancy)
        self.session.commit()
        return vacancy.id

    def insert_tg_data(self, main_vacancy_id, source, tg_id, text, salary):
        """
        Добавляет данные о вакансии из Telegram в базу данных.

        :param main_vacancy_id: ID основной вакансии
        :param source: Источник данных
        :param tg_id: Telegram ID
        :param text: Текст вакансии
        :param salary: Зарплата
        """
        tg_data = TGData(
            main_vacancy_id=main_vacancy_id,
            source=source,
            tg_id=tg_id,
            text=text,
            salary=salary
        )
        self.session.add(tg_data)
        self.session.commit()


    def insert_pl(self, name, vacancy_id, corrections_id):
        """
        Добавляет язык программирования в базу данных.

        :param name: Название языка программирования
        :param vacancy_id: ID вакансии
        :param corrections_id: ID коррекции
        """
        pl = ProgrammingLanguage(name=name, vacancy_id=vacancy_id, corrections_id=corrections_id)
        self.session.add(pl)
        self.session.commit()

    def filter_pl(self, pl):
        """
        Фильтрует языки программирования по корректным и некорректным названиям.

        :param pl: Название языка программирования
        :return: Объект LanguageCorrection или None
        """
        return self.session.query(LanguageCorrection).filter(
            (LanguageCorrection.correct == pl) | (LanguageCorrection.incorrect == pl)
        ).first()

    def insert_into_filtered_pl(self, incorrect, correct=None):
        """
        Добавляет исправление для языка программирования в базу данных.

        :param incorrect: Некорректное название
        :param correct: Корректное название (опционально)
        :return: ID добавленного исправления
        """
        correction = LanguageCorrection(correct=correct, incorrect=incorrect)
        self.session.add(correction)
        self.session.commit()
        return correction.id


    def insert_stack(self, name, vacancy_id, corrections_id):
        """
        Добавляет стек технологий в базу данных.

        :param name: Название стека технологий
        :param vacancy_id: ID вакансии
        :param corrections_id: ID коррекции
        """
        stack = Stack(name=name, vacancy_id=vacancy_id, corrections_id=corrections_id)
        self.session.add(stack)
        self.session.commit()

    def filter_stack(self, stack):
        """
        Фильтрует стеки технологий по корректным и некорректным названиям.

        :param stack: Название стека технологий
        :return: Объект StackCorrection или None
        """
        return self.session.query(StackCorrection).filter(
            (StackCorrection.correct == stack) | (StackCorrection.incorrect == stack)
        ).first()

    def insert_into_filtered_stack(self, incorrect, correct=None):
        """
        Добавляет исправление для стека технологий в базу данных.

        :param incorrect: Некорректное название
        :param correct: Корректное название (опционально)
        :return: ID добавленного исправления
        """
        correction = StackCorrection(correct=correct, incorrect=incorrect)
        self.session.add(correction)
        self.session.commit()
        return correction.id


    def close(self):
        """
        Закрывает сессию базы данных.
        """
        self.session.close()
