from langchain_openai import ChatOpenAI
from telethon import TelegramClient
from langchain.prompts import ChatPromptTemplate
from db_new import Database
from data.config import SQL_IP, SQL_USER, SQL_PASSWORD, SQL_DATABASE, TG_API_ID, TG_API_HASH, OPENAI_API_KEY
from data.prompts import prompt_1, prompt_2


db = Database(SQL_IP, SQL_USER, SQL_PASSWORD, SQL_DATABASE)

llm = ChatOpenAI(model="gpt-4o-mini", api_key=OPENAI_API_KEY)

client = TelegramClient('asosiy', TG_API_ID, TG_API_HASH).start(phone='+998942005945')

prompt_template_1 = ChatPromptTemplate.from_template(prompt_1)
prompt_template_2 = ChatPromptTemplate.from_template(prompt_2)

llm_chain_classify = prompt_template_1 | llm
llm_chain_parse = prompt_template_2 | llm
