from sqlalchemy import (
    or_, create_engine, Column, Integer, String, Float, Date, DateTime, ForeignKey, Text, func
)
from sqlalchemy.exc import OperationalError, DisconnectionError
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from datetime import timedelta
import logging
import time


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

class TGVacancy(Base):
    __tablename__ = 'tg_vacancy'
    id = Column(Integer, primary_key=True, autoincrement=True)
    text = Column(Text)
    vacancy = Column(String(255))
    hash = Column(String(64), unique=True, nullable=False)


class Database:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connect()

    def connect(self):
        self.engine = create_engine(f'mysql+mysqlconnector://{self.user}:{self.password}@{self.host}/{self.database}')
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def reconnect(self):
        try:
            logging.info("Попытка переподключения к базе данных...")
            self.connect()
            logging.info("Переподключение успешно.")
        except Exception as e:
            logging.error(f"Ошибка при переподключении: {e}")
            raise

    def close(self):
        self.session.close()

    # ========================= Check  =========================

    def check_vacancy_exists(self, source: str, tg_id: str):
        try:
            return self.session.query(TGData).filter_by(source=source, tg_id=tg_id).first() is not None
        
        except (OperationalError, DisconnectionError) as e:
            logging.error(f"Ошибка подключения при проверке вакансии: {e}")
            self.reconnect()
            logging.info("Повторная попытка выполнения запроса...")
            time.sleep(2)
            return self.check_vacancy_exists(source, tg_id)
        
        except Exception as e:
            logging.error(f"Ошибка при проверке вакансии: {e}")

    def check_duplicate(self, source, date: str, text: str):
        try:
            date_lower_bound = date - timedelta(days=30)
            filtered_texts = self.session.query(TGData.text).join(MainVacancy).filter(
                MainVacancy.date.between(date_lower_bound, date),
                MainVacancy.source_id == source
            ).all()

            filtered_texts = [item[0] for item in filtered_texts]
            return text in filtered_texts
        
        except (OperationalError, DisconnectionError) as e:
            logging.error(f"Ошибка подключения при проверке дубликатов: {e}")
            self.reconnect()
            logging.info("Повторная попытка выполнения запроса...")
            time.sleep(2)
            return self.check_duplicate(source, date, text)
        
        except Exception as e:
            logging.error(f"Ошибка при проверке дубликатов: {e}")

    def check_location(self, location_name: str) -> Location:
        try:
            result = self.session.query(Location).filter(Location.name == location_name).first()
            return result
        
        except (OperationalError, DisconnectionError) as e:
            logging.error(f"Ошибка подключения при проверке локации: {e}")
            self.reconnect()
            logging.info("Повторная попытка выполнения запроса...")
            time.sleep(2)
            return self.check_location(location_name)
        
        except Exception as e:
            logging.error(f"Ошибка при проверке локации: {e}")

    def check_company(self, company_name: str) -> Company:
        try:
            result = self.session.query(Company).filter(Company.name == company_name).first()
            return result
        
        except (OperationalError, DisconnectionError) as e:
            logging.error(f"Ошибка подключения при проверке компании: {e}")
            self.reconnect()
            logging.info("Повторная попытка выполнения запроса...")
            time.sleep(2)
            return self.check_company(company_name)
        
        except Exception as e:
            logging.error(f"Ошибка при проверке компании: {e}")

    def check_source(self, source_name: str) -> Source:
        try:
            result = self.session.query(Source).filter(Source.name == source_name).first()
            return result
        
        except (OperationalError, DisconnectionError) as e:
            logging.error(f"Ошибка подключения при проверке источника: {e}")
            self.reconnect()
            logging.info("Повторная попытка выполнения запроса...")
            time.sleep(2)
            return self.check_source(source_name)
        
        except Exception as e:
            logging.error(f"Ошибка при проверке источника: {e}")

    def check_experience(self, experience_name: str) -> Experience:
        try:
            result = self.session.query(Experience).filter(Experience.name == experience_name).first()
            return result
        
        except (OperationalError, DisconnectionError) as e:
            logging.error(f"Ошибка подключения при проверке опыта: {e}")
            self.reconnect()
            logging.info("Повторная попытка выполнения запроса...")
            time.sleep(2)
            return self.check_experience(experience_name)
        
        except Exception as e:
            logging.error(f"Ошибка при проверке опыта: {e}")

    # ========================= Filter =========================

    # Язык программирования (pl)
    def filter_pl(self, pl: str) -> (LanguageFilter | None):
        try:
            return self.session.query(LanguageFilter).filter(
                (LanguageFilter.correct == pl) | (LanguageFilter.incorrect == pl)
            ).first()
        
        except (OperationalError, DisconnectionError) as e:
            logging.error(f"Ошибка подключения при фильтрации языка программирования: {e}")
            self.reconnect()
            logging.info("Повторная попытка выполнения запроса...")
            time.sleep(2)
            return self.filter_pl(pl)
        
        except Exception as e:
            logging.error(f"Ошибка при фильтрации языка программирования: {e}")

    def insert_into_filtered_pl(self, incorrect: str) -> int:
        try:
            correction = LanguageFilter(correct='new', incorrect=incorrect)
            self.session.add(correction)
            self.session.commit()
            return correction.id
        
        except (OperationalError, DisconnectionError) as e:
            self.session.rollback()
            logging.error(f"Ошибка подключения при добавлении исправления языка программирования: {e}")
            self.reconnect()
            logging.info("Повторная попытка выполнения запроса...")
            time.sleep(2)
            return self.insert_into_filtered_pl(incorrect)
        
        except Exception as e:
            self.session.rollback()
            logging.error(f"Ошибка при добавлении исправления языка программирования: {e}")

    # Стек
    def filter_stack(self, stack: str) -> (StackFilter | None):
        try:
            return self.session.query(StackFilter).filter(
                (StackFilter.correct == stack) | (StackFilter.incorrect == stack)
            ).first()
        
        except (OperationalError, DisconnectionError) as e:
            logging.error(f"Ошибка подключения при фильтрации стека технологий: {e}")
            self.reconnect()
            logging.info("Повторная попытка выполнения запроса...")
            time.sleep(2)
            return self.filter_stack(stack)
        
        except Exception as e:
            logging.error(f"Ошибка при фильтрации стека технологий: {e}")

    def insert_into_filtered_stack(self, incorrect: str) -> int:
        try:
            correction = StackFilter(correct='new', incorrect=incorrect)
            self.session.add(correction)
            self.session.commit()
            return correction.id
        
        except (OperationalError, DisconnectionError) as e:
            self.session.rollback()
            logging.error(f"Ошибка подключения при добавлении исправления стека технологий: {e}")
            self.reconnect()
            logging.info("Повторная попытка выполнения запроса...")
            time.sleep(2)
            return self.insert_into_filtered_stack(incorrect)
        
        except Exception as e:
            self.session.rollback()
            logging.error(f"Ошибка при добавлении исправления стека технологий: {e}")

    # Основной фильтр
    def main_filter(self, category: str, position: str) -> (MainFilter | None):
        try:
            result = self.session.query(MainFilter).filter(
                or_(
                    (MainFilter.incorrect_category == category) & (MainFilter.incorrect_position == position),
                    (MainFilter.correct_category == category) & (MainFilter.correct_position == position)
                )
            ).first()
            return result
        
        except (OperationalError, DisconnectionError) as e:
            logging.error(f"Ошибка подключения при проверке основного фильтра: {e}")
            self.reconnect()
            logging.info("Повторная попытка выполнения запроса...")
            time.sleep(2)
            return self.main_filter(category, position)
        
        except Exception as e:
            logging.error(f"Ошибка при проверке основного фильтра: {e}")

    def insert_main_filter(self, incorrect_category, incorrect_position, correct_category='new', correct_position='new'):
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
        
        except (OperationalError, DisconnectionError) as e:
            self.session.rollback()
            logging.error(f"Ошибка подключения при добавлении в основной фильтр: {e}")
            self.reconnect()
            logging.info("Повторная попытка выполнения запроса...")
            time.sleep(2)
            return self.insert_main_filter(incorrect_category, incorrect_position, correct_category, correct_position)
        
        except Exception as e:
            logging.error(f"Ошибка при добавлении в основной фильтр: {e}")
            self.session.rollback()

    # ========================= Insert =========================

    def insert_main_vacancy(self, location_id, company_id, experience_id, source_id, filter_id, salary, date):
        """
        Добавляет основную вакансию в базу данных.
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
        
        except (OperationalError, DisconnectionError) as e:
            self.session.rollback()
            logging.error(f"Ошибка подключения при добавлении вакансии: {e}")
            self.reconnect()
            time.sleep(2)
            return self.insert_main_vacancy(location_id, company_id, experience_id, source_id, filter_id, salary, date)
        
        except Exception as e:
            self.session.rollback()
            logging.error(f"Ошибка при добавлении вакансии: {e}")

    def insert_tg_data(self, main_vacancy_id, tg_id, text):
        """
        Добавляет данные о вакансии из Telegram в базу данных.
        """
        try:
            tg_data = TGData(main_vacancy_id=main_vacancy_id, tg_id=tg_id, text=text)
            self.session.add(tg_data)
            self.session.commit()
        
        except (OperationalError, DisconnectionError) as e:
            self.session.rollback()
            logging.error(f"Ошибка подключения при добавлении данных Telegram: {e}")
            self.reconnect()
            time.sleep(2)
            return self.insert_tg_data(main_vacancy_id, tg_id, text)
        
        except Exception as e:
            self.session.rollback()
            logging.error(f"Ошибка при добавлении данных Telegram: {e}")

    def insert_pl(self, vacancy_id, correct_id):
        """
        Добавляет язык программирования в базу данных.
        """
        try:
            pl = ProgrammingLanguage(vacancy_id=vacancy_id, correct_id=correct_id)
            self.session.add(pl)
            self.session.commit()
        
        except (OperationalError, DisconnectionError) as e:
            self.session.rollback()
            logging.error(f"Ошибка подключения при добавлении языка программирования: {e}")
            self.reconnect()
            time.sleep(2)
            return self.insert_pl(vacancy_id, correct_id)
        
        except Exception as e:
            self.session.rollback()
            logging.error(f"Ошибка при добавлении языка программирования: {e}")

    def insert_stack(self, vacancy_id, correct_id):
        """
        Добавляет стек технологий в базу данных.
        """
        try:
            stack = Stack(vacancy_id=vacancy_id, correct_id=correct_id)
            self.session.add(stack)
            self.session.commit()
        
        except (OperationalError, DisconnectionError) as e:
            self.session.rollback()
            logging.error(f"Ошибка подключения при добавлении стека технологий: {e}")
            self.reconnect()
            time.sleep(2)
            return self.insert_stack(vacancy_id, correct_id)
        
        except Exception as e:
            self.session.rollback()
            logging.error(f"Ошибка при добавлении стека технологий: {e}")

    def insert_location(self, location: str):
        """
        Вставляет новое местоположение в таблицу `location`.
        """
        try:
            new_location = Location(name=location)
            self.session.add(new_location)
            self.session.commit()
            return new_location.id
        
        except (OperationalError, DisconnectionError) as e:
            self.session.rollback()
            logging.error(f"Ошибка подключения при добавлении местоположения: {e}")
            self.reconnect()
            time.sleep(2)
            return self.insert_location(location)
        
        except Exception as e:
            self.session.rollback()
            logging.error(f"Ошибка при добавлении местоположения: {e}")

    def insert_company(self, company: str):
        """
        Вставляет новую компанию в таблицу `company`.
        """
        try:
            new_company = Company(name=company)
            self.session.add(new_company)
            self.session.commit()
            return new_company.id
        
        except (OperationalError, DisconnectionError) as e:
            self.session.rollback()
            logging.error(f"Ошибка подключения при добавлении компании: {e}")
            self.reconnect()
            time.sleep(2)
            return self.insert_company(company)
        
        except Exception as e:
            self.session.rollback()
            logging.error(f"Ошибка при добавлении компании: {e}")

    def insert_source(self, source: str):
        """
        Вставляет новый источник в таблицу `source`.
        """
        try:
            new_source = Source(name=source)
            self.session.add(new_source)
            self.session.commit()
            return new_source.id
        
        except (OperationalError, DisconnectionError) as e:
            self.session.rollback()
            logging.error(f"Ошибка подключения при добавлении источника: {e}")
            self.reconnect()
            time.sleep(2)
            return self.insert_source(source)
        
        except Exception as e:
            self.session.rollback()
            logging.error(f"Ошибка при добавлении источника: {e}")

    def insert_experience(self, experience: str):
        """
        Вставляет новый опыт в таблицу `experience`.
        """
        try:
            new_experience = Experience(name=experience)
            self.session.add(new_experience)
            self.session.commit()
            return new_experience.id
        
        except (OperationalError, DisconnectionError) as e:
            self.session.rollback()
            logging.error(f"Ошибка подключения при добавлении опыта: {e}")
            self.reconnect()
            time.sleep(2)
            return self.insert_experience(experience)
        
        except Exception as e:
            self.session.rollback()
            logging.error(f"Ошибка при добавлении опыта: {e}")


    def insert_exchange_rate(self, rate: float):
        """
        Обновляет курс валюты в базе данных.
        """
        try:
            self.session.query(ExchangeRate).delete()
            exchange_rate = ExchangeRate(rate=rate)
            self.session.add(exchange_rate)
            self.session.commit()
            logging.info(f"Обновлено: новый курс валюты {rate} добавлен.")
            
        except (OperationalError, DisconnectionError) as e:
            self.session.rollback()
            logging.error(f"Ошибка подключения при обновлении курса валют: {e}")
            self.reconnect()
            logging.info("Попытка переподключения и повторное выполнение запроса...")
            time.sleep(2)
            return self.insert_exchange_rate(rate)

        except Exception as e:
            self.session.rollback()
            logging.error(f"Ошибка при обновлении курса валют: {e}")
