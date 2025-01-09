from sqlalchemy import (
    or_, create_engine, Column, Integer, String, Float, Date, DateTime, ForeignKey, Text, func
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
import logging
from datetime import timedelta


Base = declarative_base()

class MainVacancy(Base):
    __tablename__ = 'main_vacancy'
    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, ForeignKey('location.id', ondelete="CASCADE", onupdate="CASCADE"))
    company_id = Column(Integer, ForeignKey('company.id', ondelete="CASCADE", onupdate="CASCADE"))
    experience_id = Column(Integer, ForeignKey('experience.id', ondelete="CASCADE", onupdate="CASCADE"))
    source_id = Column(Integer, ForeignKey('source.id', ondelete="CASCADE", onupdate="CASCADE"))
    filter_id = Column(Integer, ForeignKey('main_filter.id', ondelete="CASCADE", onupdate="CASCADE"))
    salary = Column(Float)
    date = Column(Date)

    location = relationship('Location')
    company = relationship('Company')
    source = relationship('Source')
    experience = relationship('Experience')
    filter = relationship('MainFilter')
    programming_languages = relationship('ProgrammingLanguage', back_populates='main_vacancy')
    stacks = relationship('Stack', back_populates='main_vacancy')

class MainFilter(Base):
    __tablename__ = 'main_filter'
    id = Column(Integer, primary_key=True, autoincrement=True)
    correct_category = Column(String(255))
    incorrect_category = Column(String(255))
    correct_position = Column(String(255))
    incorrect_position = Column(String(255))
    date = Column(Date, default=func.current_date())

class TGData(Base):
    __tablename__ = 'tg_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    main_vacancy_id = Column(Integer, ForeignKey('main_vacancy.id', ondelete="CASCADE", onupdate="CASCADE"))
    tg_id = Column(Integer)
    text = Column(Text)

class ExchangeRate(Base):
    __tablename__ = 'exchange_rates'
    rate = Column(Float, primary_key=True)
    timestamp = Column(DateTime, server_default=func.now())

class ProgrammingLanguage(Base):
    __tablename__ = 'programming_language'
    id = Column(Integer, primary_key=True, autoincrement=True)
    correct_id = Column(Integer, ForeignKey('language_filter.id', ondelete="CASCADE", onupdate="CASCADE"))
    vacancy_id = Column(Integer, ForeignKey('main_vacancy.id', ondelete="CASCADE", onupdate="CASCADE"))

    main_vacancy = relationship('MainVacancy', back_populates='programming_languages')
    language_filter = relationship('LanguageFilter')

class LanguageFilter(Base):
    __tablename__ = 'language_filter'
    id = Column(Integer, primary_key=True, autoincrement=True)
    correct = Column(String(255))
    incorrect = Column(String(255))
    date = Column(Date, default=func.current_date())

class Stack(Base):
    __tablename__ = 'stack'
    id = Column(Integer, primary_key=True, autoincrement=True)
    vacancy_id = Column(Integer, ForeignKey('main_vacancy.id', ondelete="CASCADE", onupdate="CASCADE"))
    correct_id = Column(Integer, ForeignKey('stack_filter.id', ondelete="CASCADE", onupdate="CASCADE"))

    main_vacancy = relationship('MainVacancy', back_populates='stacks')
    stack_filter = relationship('StackFilter')

class StackFilter(Base):
    __tablename__ = 'stack_filter'
    id = Column(Integer, primary_key=True, autoincrement=True)
    correct = Column(String(255))
    incorrect = Column(String(255))
    date = Column(Date, default=func.current_date())

class Location(Base):
    __tablename__ = 'location'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), unique=True, nullable=False)
    date = Column(Date, default=func.current_date())

class Company(Base):
    __tablename__ = 'company'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), unique=True, nullable=False)
    date = Column(Date, default=func.current_date())

class Source(Base):
    __tablename__ = 'source'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), unique=True, nullable=False)
    date = Column(Date, default=func.current_date())

class Experience(Base):
    __tablename__ = 'experience'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), unique=True, nullable=False)
    date = Column(Date, default=func.current_date())


class Database:
    def __init__(self, host, user, password, database):
        """
        Инициализация подключения к базе данных.

        :param host: Хост базы данных
        :param user: Имя пользователя
        :param password: Пароль пользователя
        :param database: Название базы данных
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connect()

    def connect(self):
        """Устанавливает соединение с базой данных."""
        self.engine = create_engine(f'mysql+mysqlconnector://{self.user}:{self.password}@{self.host}/{self.database}')
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def reconnect(self):
        """Переподключение к базе данных в случае сбоя соединения."""
        try:
            logging.info("Попытка переподключения к базе данных...")
            self.connect()
            logging.info("Переподключение успешно.")
        except Exception as e:
            logging.error(f"Ошибка при переподключении: {e}")

    def close(self):
        """
        Закрывает сессию базы данных.
        """
        self.session.close()

    # ========================= Check  =========================

    def check_vacancy_exists(self, source: str, tg_id: str):
        """
        Проверяет, существует ли вакансия с указанным источником и Telegram ID.

        :param source: Источник вакансии
        :param tg_id: Telegram ID вакансии
        :return: True, если вакансия существует, иначе False
        """
        return self.session.query(TGData).filter_by(source=source, tg_id=tg_id).first() is not None

    def check_duplicate(self, source, date: str, text: str):
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
            MainVacancy.source_id == source
        ).all()

        filtered_texts = [item[0] for item in filtered_texts]
        return text in filtered_texts


    def check_location(self, location_name: str) -> Location:
        """
        Проверяет, существует ли локация в базе данных.
        :param location_name: Название локации.
        :return: Объект Location или None.
        """
        try:
            result = self.session.query(Location).filter(Location.name == location_name).first()
            return result
        except SQLAlchemyError as e:
            logging.error(f"Database error: {e}")
            self.reconnect()
        except Exception as e:
            logging.error(f"Unexpected error: {e}")

    def check_company(self, company_name: str) -> Company:
        """
        Проверяет, существует ли компания в базе данных.
        :param company_name: Название компании.
        :return: Объект Company или None.
        """
        try:
            result = self.session.query(Company).filter(Company.name == company_name).first()
            return result
        except SQLAlchemyError as e:
            logging.error(f"Database error: {e}")
            self.reconnect()
        except Exception as e:
            logging.error(f"Unexpected error: {e}")

    def check_source(self, source_name: str) -> Source:
        """
        Проверяет, существует ли источник в базе данных.
        :param source_name: Название источника.
        :return: Объект Source или None.
        """
        try:
            result = self.session.query(Source).filter(Source.name == source_name).first()
            return result
        except SQLAlchemyError as e:
            logging.error(f"Database error: {e}")
            self.reconnect()
        except Exception as e:
            logging.error(f"Unexpected error: {e}")

    def check_experience(self, experience_name: str) -> Experience:
        """
        Проверяет, существует ли уровень опыта в базе данных.
        :param experience_name: Название уровня опыта.
        :return: Объект Experience или None.
        """
        try:
            result = self.session.query(Experience).filter(Experience.name == experience_name).first()
            return result
        except SQLAlchemyError as e:
            logging.error(f"Database error: {e}")
            self.reconnect()
        except Exception as e:
            logging.error(f"Unexpected error: {e}")

    # ========================= Filter =========================

    #  Programming languagae (pl)
    def filter_pl(self, pl: str) -> (LanguageFilter | None):
        """
        Фильтрует языки программирования по корректным и некорректным названиям.

        :param pl: Название языка программирования
        :return: Объект LanguageFilter или None
        """
        return self.session.query(LanguageFilter).filter(
            (LanguageFilter.correct == pl) | (LanguageFilter.incorrect == pl)
        ).first()

    def insert_into_filtered_pl(self, incorrect: str) -> int:
        """
        Добавляет исправление для языка программирования в базу данных.

        :param incorrect: Некорректное название
        :return: ID добавленного исправления
        """
        correction = LanguageFilter(correct='new', incorrect=incorrect)
        self.session.add(correction)
        self.session.commit()
        return correction.id

    # Stack
    def filter_stack(self, stack: str) -> (StackFilter | None):
        """
        Фильтрует стеки технологий по корректным и некорректным названиям.

        :param stack: Название стека технологий
        :return: Объект StackFilter или None
        """
        return self.session.query(StackFilter).filter(
            (StackFilter.correct == stack) | (StackFilter.incorrect == stack)
        ).first()

    def insert_into_filtered_stack(self, incorrect: str) -> int:
        """
        Добавляет исправление для стека технологий в базу данных.

        :param incorrect: Некорректное название
        :return: ID добавленного исправления
        """
        correction = StackFilter(correct='new', incorrect=incorrect)
        self.session.add(correction)
        self.session.commit()
        return correction.id

    # Main filter
    def main_filter(self, category: str, position: str) -> (MainFilter | None):
        """
        Проверяет данные в таблице main_filter.
        :param category: Категория.
        :param position: Позиция.
        :return: Объект MainFilter или None.
        """
        try:
            result = self.session.query(MainFilter).filter(
                or_(
                    (MainFilter.incorrect_category == category) & (MainFilter.incorrect_position == position),
                    (MainFilter.correct_category == category) & (MainFilter.correct_position == position)
                )
            ).first()
            return result
        except SQLAlchemyError as e:
            logging.error(f"Database error: {e}")
            self.reconnect()
        except Exception as e:
            logging.error(f"Unexpected error: {e}")

    def insert_main_filter(self, incorrect_category, incorrect_position, correct_category='new', correct_position='new'):
        """
        Вставляет данные в таблицу main_filter.
        :param incorrect_category: Некорректная категория.
        :param incorrect_position: Некорректная позиция.
        :param correct_category: Корректная категория (по умолчанию 'new').
        :param correct_position: Корректная позиция (по умолчанию 'new').
        :return: ID последней вставленной строки.
        """
        try:
            new_filter = MainFilter(
                incorrect_category=incorrect_category,
                incorrect_position=incorrect_position,
                correct_category=correct_category,
                correct_position=correct_position
            )
            self.session.add(new_filter)
            self.session.commit()
            return new_filter.id
        except SQLAlchemyError as e:
            logging.error(f"Database error: {e}")
            self.reconnect()
        except Exception as e:
            logging.error(f"Unexpected error: {e}")

    # ========================= Insert =========================

    def insert_main_vacancy(self, location_id, company_id, experience_id, source_id, filter_id, salary, date):
        """
        Добавляет основную вакансию в базу данных.

        :param location_id: ID местоположения
        :param company_id: ID компании
        :param experience_id: ID опыта
        :param source_id: ID источника
        :param filter_id: ID фильтра
        :param salary: Зарплата
        :param date: Дата вакансии (опционально)
        :return: ID добавленной вакансии
        """
        try:
            vacancy = MainVacancy(
                location_id=location_id,
                company_id=company_id,
                experience_id=experience_id,
                source_id=source_id,
                filter_id=filter_id,
                salary=salary,
                date=date
            )
            self.session.add(vacancy)
            self.session.commit()
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


    def insert_location(self, location: str):
        """
        Вставляет новое местоположение в таблицу `location`.
        :param location: Название местоположения.
        :return: ID вставленного местоположения.
        """
        try:
            new_location = Location(name=location)
            self.session.add(new_location)
            self.session.commit()
            return new_location.id
        except SQLAlchemyError as e:
            logging.error(f"Database error: {e}")
            self.reconnect()
        except Exception as e:
            logging.error(f"Unexpected error: {e}")

    def insert_company(self, company: str):
        """
        Вставляет новую компанию в таблицу `company`.
        :param company: Название компании.
        :return: ID вставленной компании.
        """
        try:
            new_company = Company(name=company)
            self.session.add(new_company)
            self.session.commit()
            return new_company.id
        except SQLAlchemyError as e:
            logging.error(f"Database error: {e}")
            self.reconnect()
        except Exception as e:
            logging.error(f"Unexpected error: {e}")

    def insert_source(self, source: str):
        """
        Вставляет новый источник в таблицу `source`.
        :param source: Название источника.
        :return: ID вставленного источника.
        """
        try:
            new_source = Source(name=source)
            self.session.add(new_source)
            self.session.commit()
            return new_source.id
        except SQLAlchemyError as e:
            logging.error(f"Database error: {e}")
            self.reconnect()
        except Exception as e:
            logging.error(f"Unexpected error: {e}")

    def insert_experience(self, experience: str):
        """
        Вставляет новый опыт в таблицу `experience`.
        :param experience: Название опыта.
        :return: ID вставленного опыта.
        """
        try:
            new_experience = Experience(name=experience)
            self.session.add(new_experience)
            self.session.commit()
            return new_experience.id
        except SQLAlchemyError as e:
            logging.error(f"Database error: {e}")
            self.reconnect()
        except Exception as e:
            logging.error(f"Unexpected error: {e}")


    def insert_exchange_rate(self, rate):
        """
        Обновляет курс валюты в базе данных.

        :param rate: Новый курс валюты
        """
        try:
            self.session.query(ExchangeRate).delete()
            exchange_rate = ExchangeRate(rate=rate)
            self.session.add(exchange_rate)
            self.session.commit()
            logging.info(f"Обновлено: новый курс валюты {rate} добавлен.")
        except Exception as e:
            self.session.rollback()
            logging.error(f"Ошибка при обновлении курса валют: {e}")
