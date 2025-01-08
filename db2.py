from sqlalchemy import (
    create_engine, Column, Integer, String, Float, Date, DateTime, ForeignKey, JSON, Text
)
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
import logging
from datetime import datetime, timedelta


Base = declarative_base()

class ExchangeRate(Base):
    __tablename__ = 'exchange_rates'
    id = Column(Integer, primary_key=True)
    rate = Column(Float)
    timestamp = Column(DateTime)

class Location(Base):
    __tablename__ = 'location'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)

class Company(Base):
    __tablename__ = 'company'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)

class Source(Base):
    __tablename__ = 'source'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)

class Experience(Base):
    __tablename__ = 'experience'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)

class MainFilter(Base):
    __tablename__ = 'main_filter'
    id = Column(Integer, primary_key=True, autoincrement=True)
    correct_category = Column(String)
    incorrect_category = Column(String)
    correct_position = Column(String)
    incorrect_position = Column(String)

class MainVacancy(Base):
    __tablename__ = 'main_vacancy'
    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, ForeignKey('location.id', ondelete="CASCADE", onupdate="CASCADE"))
    category = Column(String)
    position = Column(String)
    company_id = Column(Integer, ForeignKey('company.id', ondelete="CASCADE", onupdate="CASCADE"))
    experience_id = Column(Integer, ForeignKey('experience.id', ondelete="CASCADE", onupdate="CASCADE"))
    salary = Column(Float)
    date = Column(Date, default=datetime.utcnow().date())
    source_id = Column(Integer, ForeignKey('source.id', ondelete="CASCADE", onupdate="CASCADE"))
    filter_id = Column(Integer, ForeignKey('main_filter.id', ondelete="CASCADE", onupdate="CASCADE"))

    location = relationship('Location')
    company = relationship('Company')
    source = relationship('Source')
    experience = relationship('Experience')
    filter = relationship('MainFilter')
    programming_languages = relationship('ProgrammingLanguage', back_populates='main_vacancy')
    stacks = relationship('Stack', back_populates='main_vacancy')

class LanguageFilter(Base):
    __tablename__ = 'language_filter'
    id = Column(Integer, primary_key=True, autoincrement=True)
    correct = Column(String)
    incorrect = Column(String)
    date = Column(Date, default=datetime.utcnow().date())

class ProgrammingLanguage(Base):
    __tablename__ = 'programming_language'
    id = Column(Integer, primary_key=True, autoincrement=True)
    correct_id = Column(Integer, ForeignKey('language_filter.id', ondelete="CASCADE", onupdate="CASCADE"))
    vacancy_id = Column(Integer, ForeignKey('main_vacancy.id', ondelete="CASCADE", onupdate="CASCADE"))

    main_vacancy = relationship('MainVacancy', back_populates='programming_languages')
    language_filter = relationship('LanguageFilter')

class StackFilter(Base):
    __tablename__ = 'stack_filter'
    id = Column(Integer, primary_key=True, autoincrement=True)
    correct = Column(String)
    incorrect = Column(String)
    date = Column(Date, default=datetime.utcnow().date())

class Stack(Base):
    __tablename__ = 'stack'
    id = Column(Integer, primary_key=True, autoincrement=True)
    vacancy_id = Column(Integer, ForeignKey('main_vacancy.id', ondelete="CASCADE", onupdate="CASCADE"))
    correct_id = Column(Integer, ForeignKey('stack_filter.id', ondelete="CASCADE", onupdate="CASCADE"))

    main_vacancy = relationship('MainVacancy', back_populates='stacks')
    stack_filter = relationship('StackFilter')

class TGData(Base):
    __tablename__ = 'tg_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    main_vacancy_id = Column(Integer, ForeignKey('main_vacancy.id', ondelete="CASCADE", onupdate="CASCADE"))
    tg_id = Column(Integer)
    text = Column(Text)


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
            logging.info(f"Обновлено: новый курс валюты {rate} добавлен.")
        except Exception as e:
            self.session.rollback()
            logging.error(f"Ошибка при обновлении курса валют: {e}")

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

    def insert_main_vacancy(self, location_id, category, position, company_id, experience_id, salary, source_id, filter_id, date):
        """
        Добавляет основную вакансию в базу данных.

        :param location_id: ID местоположения
        :param category: Категория вакансии
        :param position: Должность
        :param company_id: ID компании
        :param experience_id: ID опыта
        :param salary: Зарплата
        :param source_id: ID источника
        :param filter_id: ID фильтра
        :param date: Дата вакансии (опционально)
        :return: ID добавленной вакансии
        """
        try:
            vacancy = MainVacancy(
                location_id=location_id,
                category=category,
                position=position,
                company_id=company_id,
                experience_id=experience_id,
                salary=salary,
                source_id=source_id,
                filter_id=filter_id,
                date=date
            )
            self.session.add(vacancy)
            self.session.commit()
            logging.info(f"Вакансия {position} добавлена.")
            return vacancy.id
        except Exception as e:
            self.session.rollback()
            logging.error(f"Ошибка при добавлении вакансии: {e}")

    def insert_tg_data(self, main_vacancy_id, tg_id, text):
        """
        Добавляет данные о вакансии из Telegram в базу данных.

        :param main_vacancy_id: ID основной вакансии
        :param tg_id: Telegram ID
        :param text: Текст вакансии
        """
        try:
            tg_data = TGData(main_vacancy_id=main_vacancy_id, tg_id=tg_id, text=text)
            self.session.add(tg_data)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            logging.error(f"Ошибка при добавлении данных Telegram: {e}")

    def insert_pl(self, vacancy_id, correct_id):
        """
        Добавляет язык программирования в базу данных.

        :param vacancy_id: ID вакансии
        :param correct_id: ID корректного названия языка
        """
        try:
            pl = ProgrammingLanguage(vacancy_id=vacancy_id, correct_id=correct_id)
            self.session.add(pl)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            logging.error(f"Ошибка при добавлении языка программирования: {e}")

    def filter_pl(self, pl):
        """
        Фильтрует языки программирования по корректным и некорректным названиям.

        :param pl: Название языка программирования
        :return: Объект LanguageFilter или None
        """
        return self.session.query(LanguageFilter).filter(
            (LanguageFilter.correct == pl) | (LanguageFilter.incorrect == pl)
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

    def insert_stack(self, vacancy_id, correct_id):
        """
        Добавляет стек технологий в базу данных.

        :param vacancy_id: ID вакансии
        :param correct_id: ID корректного названия стека
        """
        try:
            stack = Stack(vacancy_id=vacancy_id, correct_id=correct_id)
            self.session.add(stack)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            logging.error(f"Ошибка при добавлении стека технологий: {e}")

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
