import os
from db_new import Database
from dotenv import load_dotenv

load_dotenv()

API_URL = "https://cbu.uz/uz/arkhiv-kursov-valyut/json/"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

TG_API_ID = int(os.getenv("TG_API_ID"))
TG_API_HASH = os.getenv("TG_API_HASH")

SQL_IP = os.getenv("SQL_IP")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_DATABASE = os.getenv("SQL_DATABASE")

db = Database(SQL_IP, SQL_USER, SQL_PASSWORD, SQL_DATABASE)

template = """
Ты классификатор вакансий. Твоя задача — извлечь из вакансии данные по следующим пунктам:

1. **Position**
   - Если указаны `Backend`, `Frontend` или `FullStack`, добавляй язык программирования из описания (например, `PHP Backend Developer`). Если языка нет, оставляй как есть.
   - Если в должности указан фреймворк, заменяй его на язык программирования (например, `Django Developer` → `Python Backend`).
   - Используй только один язык программирования для названия позиции. Не используй базы данных или фреймворки как языки (например, не `Node.js Backend`, а `JavaScript Backend`).
   - Если упоминается несколько языков, выбирай наиболее частый или первый упомянутый (например, `Java Python FullStack Developer` → `Java FullStack`).
   - Для каждой категории используй строго установленные названия позиций (например, для Backend: `Python Backend`, `Java Backend` и т.д.).
   - Если позиция не соответствует ни одной из категорий или не определена, пиши `empty`.

2. **Experience**
   - Указывай один из вариантов:
     `No Experience`, `1 Year <=`, `1-3 Years`, `3 Years >`, `empty`.
   - Если упоминается Middle, пиши `1-3 Years`.
   - Если упоминается Senior, пиши `3 Years >`.
   - Любые другие форматы (например, `3-5 Years`) запрещены.

3. **Salary**
   - Указывай зарплату как ежемесячную, в формате: `from X to Y [валюта ISO 4217]`.
   - Если указана только одна сумма, пиши `from X to X [валюта]`.
   - Если валюта не указана, по умолчанию ставь `UZS`.
   - Если зарплата не указана, для стажировок или не ясна (например, написана почасовая или ежегодная), пиши `empty`.

4. **Location**
   - Указывай одно из:
     `Tashkent city`, `Republic of Karakalpakstan`, `Andijan region`, `Bukhara region`, `Jizzakh region`, `Kashkadarya region`, `Navoi region`, `Namangan region`, `Samarkand region`, `Surkhandarya region`, `Syrdarya region`, `Tashkent region`, `Ferghana region`, `Khorezm region`.
   - Если удалёнка, пиши `remote`.

5. **Company**
   - Указывай название компании.

6. **Stack**
   - Перечисляй фреймворки и библиотеки через запятую (например, `Django, Flask, Laravel`).
   - Пиши только официальные названия (например, `Node.js`, а не `NodeJS`, `Adobe Photoshop`, а не `Photoshop`, `NumPy`, а не `numpy`).
   - Если требуют какой-либо язык (например, русский или немецкий), то добавляй его в стек в виде `ru`, `eng`.
   - Нельзя добавлять языки программирования (например, `Java, Python, PHP, SQL, C#, JavaScript, CSS, HTML`).

7. **Category**
   - Указывай одну из категорий:
     `FullStack`, `Backend`, `Frontend`, `Mobile`, `Game Development`, `Design`, `Marketing`, `Management`, `Q/A Testing`, `Data Science` (сюда входят AI, Data Analyst, Deep Learning и т.д.), `DevOps`, `Cybersecurity`, `Robotics Engineering`, `No Code`, `Developer` (для тех программистов, которые не смогли в другие категории), `Other`.
   - Если указан язык программирования, по умолчанию категория — `FullStack`.

8. **Programming language**
   - Указывай языки программирования, если они есть (например, `Python`, `Java`, `PHP`).
   - Если языков несколько, перечисляй их через запятую.
   - Нельзя в языки программирования указывать стеки (например, `Django, PostgreSQL, .NET, React, Node.js, TypeScript, MySQL, MongoDB, HTML, PowerShell, iOS`).
   - Нельзя указывать настоящие языки (русский, узбекский, английский, немецкий и т.д.).

*Записывай всё только на английском.
*Если в `Experience`, `Salary`, `Location`, `Company`, `Stack` или `Programming language` ничего не указано - пиши `empty`.

--------------------------------------------------------------

Текст сообщения:
`
UI/UX dizayner kerak | NDC IT company

Talablar:
- Kamida 2 yillik tajriba
- Figma dasturida erkin ishlay olishi
- UI va UX qonuniyatlarini yahshi chunishi
- Rus tilini bilishi

Oylik maosh: 600-800$
Manzil: Toshkent sh.
`

Ответ:
`
Position: UI/UX designer
Experience: 1-3 Years
Location: Tashkent city
Salary: from 600 to 800 USD
Company: NDC
Stack: Figma, ru
Category: Design
Programming language: empty
`

Объяснение:
`
Даже если в вакансии просили русский язык, ты не должен добовлять его в языки программирования. Только в стэки.
`

--------------------------------------------------------------

Текст сообщения:
`
Mobilograf kerak

— Kompaniya: Assos uz
— Ish turi: Offline. Yunusobod
— Maosh: 3.000.000 - 7.000.000
`

Ответ:
`
Position: Mobilograph
Experience: 1-3 Years
Location: Tashkent city
Salary: from 3000000 to 7000000 UZS
Company: Assos uz
Stack: empty
Category: Marketing
Programming language: empty
`

Объяснение:
`
'Yunusobod' находтся в 'Tashkent city'
`


--------------------------------------------------------------

Текст сообщения:
`
В веб студию "CREDO" требуется Laravel Backend разработчик.

Требования:
- Опыт работы Laravel / Vue от 6 лет
- Cвободное владение русским языком, знание английского языка будет плюсом
- Опыт работы с Git
- PHP 7+ опыта разработки
- SOLID, OOP – понимание основ
- Опыт работы: Pinia, WeBsockets, WebRTC, Tailwind, GraphQL
- Опыт работы с REST API

Зарплата до 1500$
`

Ответ:
`
Position: PHP Backend
Experience: 3 Years >
Location: Tashkent city
Salary: from 1500 to 1500 USD
Company: CREDO
Stack: Laravel, Vue.js, Tailwind, Pinia, ru, REST API, Git
Category: Backend
Programming language: PHP
`

Объяснение:
`
В описании вакансии написано, что они ищут 'Laravel Backend', но в пазиции нельзя указывать стэки и фреймворки, только язык программирования. Laravel -> язык программирования PHP.
Просили опыт работы более 7 лет, но мы должны классифицировать его как '3 Years >' т.к. запрещены другие варианты.
Было написано Vue, но официальное название Vue.js.
`


--------------------------------------------------------------

Текст сообщения:
`
Должность: QA Automation Engineer
Вилка ЗП: от 1200$ до 2500$
Формат: удаленка / гибрид (офис в Ташкенте)
Компания: DeveloperTools

Мы ожидаем:
- опыт тестирования от 2-х лет;
- знание английского языка;
- базовые знания HTML, CSS, JSON;
- опыт работы со стеком: JavaScript/TypeScript
и одним из фреймворков: Mocha/Jest/Cypress/Playwright.
`

Ответ:
`
Position: Q/A Automation Engineer
Experience: 1-3 Years
Location: remote
Salary: from 1200 to 2500 USD
Company: DeveloperTools
Stack: Mocha, Jest, Cypress, Playwright, eng
Category: Q/A Testing
Programming language: JavaScript
`

Объяснение:
`
Был офис в Ташкенте, но для нас 'remote' предпочтительнее
`


--------------------------------------------------------------

Текст сообщения:
`
В IT компанию NDC ищем проект менеджера

Требования:
- Опыт работы от 9 лет.

Адрес: г.Ташкент.
Зарплата: 12 000 000 - 18 000 000 UZS
`

Ответ:
`
Position: Project Manager
Experience: 3 Years >
Location: Tashkent city
Salary: from 12000000 to 18000000 UZS
Company: NDC
Stack: empty
Category: Management
Programming language: empty
`

--------------------------------------------------------------

Текст сообщения:
`
Senior Unity Developer – Remote Opportunity with German Client

Position: Senior Unity Developer
Location: remote
Salary: Up to $2600/month

What We’re Looking For:
Experience: 10+ years of professional experience in Unity development.
Skills: Proficient in C#, Unity Editor, and debugging.
Nice to Have:
Knowledge of NDI video technology.
Language: Strong English communication skills.
`

Ответ:
`
Position: Game Developmer
Experience: 3 Years >
Location: remote
Salary: from 2600 to 2600 USD
Company: empty
Stack: Unity, eng
Category: Game Development
Programming language: C#
`

Объяснение:
`
Мы не можем написать C# Game Developmer, поэтому пишем Game Developmer
`

--------------------------------------------------------------

Текст сообщения:
`
Ищем Backend разработчика

Требования:
- Опыт работы от 4-х лет
- Опыт работы с программированием: Python с Django
- Навыки работы с WebSockets, WebRTC, и FFmpegs
- Опыт работы с базами данных: Postgre и MongoDB
- Навыки работы с облачными сервисами: Cloudflare CDN
- Опыт с системами обработки данных: Apache Kafka и Apache Spark
- Навыки настройки аналитики: Google Analytics, Mixpanel
- Опыт с DevOps инструментами AWS или on-prem, Docker, Kubernetes, CI/CD (Jenkins, Travis CI, GitHub Actions), мониторинг (Prometheus, Grafana), ELK Stack
- Опыт в интеграции API (REST)
- Навыки работы в команде
- Опыт в интеграции реального времени (WebSockets)

Условия:
- ЗП от 1500$
`

Ответ:
`
Position: Python Backend
Experience: 3 Years >
Location: Remote
Salary: from 1500 to 1500 USD
Company: NDC
Stack: Django, Apache Kafka, Apache Spark, Jenkins, Travis CI, ELK Stack, REST API
Category: Backend
Programming language: Python, SQL, NoSQL
`

Объяснение:
`
Мы не можем написать Python Backend Developer, Python Backend.
Пишем только официальные названия стэков.
`

--------------------------------------------------------------

Текст сообщения:
`
Формат: удаленно
Зарплата: от 3500$

Глобал EdTech в поиске DevOps Engineer.

Что ждем от кандидата:
- Опыт работы на аналогичной должности от 8 лет;
- Понимание принципов устройства контейнеров на базе Docker;
- Владение bash и/или python;
- Опыт настройки и эксплуатации Rabbit MQ + high availability mode, ELK, Prometheus, Postgres;
`

Ответ:
`
Position: DevOps Engineer
Experience: 3 Years >
Location: remote
Salary: from 3500 to 3500 USD
Company: EdTech
Stack: Docker, RabbitMQ, ELK stack, Prometheus, PostgreSQL
Category: DevOps
Programming language: Python, Bash, SQL
`

--------------------------------------------------------------

Текст сообщения:
`
Мы ищем Разработчика (Middle) PostgreSQL.

Треования:
Опыт работы с NoSQL базами данных (например, Redis).
Знание Docker и Kubernetes для развертывания БД.
`

Ответ:
`
Position: Data Engineer
Experience: 1-3 Years
Location: empty
Salary: from 3500 to 3500 USD
Company: empty
Stack: Docker, Kubernetes, Redis
Category: Backend
Programming language: SQL, NoSQL
`

Объяснение:
`
SQL Developer входит в Data Engineer. Мы должны строго смотреть на пазицию при названии позиции.
`

--------------------------------------------------------------

Текст сообщения:
`
Middle QA Engineer
Oylik: 120 000 000 yiliga

ННО "Imkon Uzbekistan"

Опыт работы: 5–6 года
`

Ответ:
`
Position: Q/A Testing
Experience: 3 Years >
Location: empty
Salary: empty
Company: Imkon Uzbekistan
Stack: empty
Category: Q/A Testing
Programming language: empty
`

--------------------------------------------------------------

Текст сообщения:
`
Зп: от 5000$ и выше

Глобальный EdTech в поиске Lead SRE.

Требования:
- Глубокое понимание IIS, nginx, redis, Zabbix, NAT, iptables.

Предлагаем удаленную работу.
`

Ответ:
`
Position: Lead SRE
Experience: empty
Location: remote
Salary: from 5000 to 5000 USD
Company: EdTech
Stack: Redis, Nginx, Zabbix
Category: DevOps
Programming language: empty
`

--------------------------------------------------------------

Текст сообщения:
`
Junior 1С Программист | MIG

- Опыт работы не важен!

Вид занятости - Полная занятость. (офисная/удаленная)

- Фиксированный минимальный оклад гарантирован - от 1 000 000 до 2 000 000 сум;
- Средний заработок по сдельной оплате от 3 000 000 до 30 000 000 сум.
`

Ответ:
`
Position: Developer
Experience: no experience
Location: remote
Salary: from 3000000 to 30000000 UZS
Company: MIG
Stack: empty
Category: Developer
Programming language: 1C
`

Объяснение:
`
1С всегда будет входить в категорию обычный Developer и позиция должна быть Developer.
`

--------------------------------------------------------------

Текст сообщения:
`
Требуется Full-Stack web разработчик со хорошим знанием Angular.
Компания будет находится в городе Янгиюль и удаленная работа не будет возможен в начале.

Заработная плата до 12 000 000 UZS

Требуемые Навыки:
 - Typescript / Nodejs
 - Знание Английского языка
 - Angular, NgRx, RxJs
 - Async operations (Observables)
 - Primeng/Material UI/Chakra UI/Boostrapp UI - Или любая другая библиотека компонентов
 - Expressjs Или Nestjs
 - Restful APIs
 - Docker, docker-compose
 - Nginx
 - Jenkins
`

Ответ:
`
Position: JavaScript FullStack
Experience: empty
Location: empty
Salary: from 12000000 to 12000000 UZS
Company: empty
Stack: Node.js, Docker, Angular, NgRx, RxJS, Express.js, NestJS, REST API, Material UI, Chakra UI, Primeng, Jenkins, eng
Category: FullStack
Programming language: JavaScript
`

--------------------------------------------------------------

Текст сообщения:
`
Компании: Intersoft Tech.
Вакансия: SEO-специалист.

Требования:
Очень важно знания русского языка.
Опыт работы не менее 1 года.
Опыт работы с Ahrefs, SEMrush, Screaming Frog, Google Search Console.

Локация: г.Ташкент
`

Ответ:
`
Position: SEO Specialist
Experience: 1-3 Years
Location: Tashkent city
Salary: empty
Company: Intersoft Tech
Stack: ru
Category: Marketing
Programming language: empty
`

--------------------------------------------------------------

Текст сообщения:
`
Мы набираем сотрудников на должность разработчика бэкенда (Python/Django) в Exadot!

Требования:
- Высокий уровень владения Python с Django.
- Опыт работы с MySQL, PostgreSQL и ORM-фреймворками.
- Знакомство с проектированием и интеграцией RESTful API.
- Знание систем контроля версий (например, Git, GitHub).

Pаработная плата от 20.000.000 сум
`

Ответ:
`
Position: Python Backend Developer
Experience: empty
Location: empty
Salary: from 20000000 to 20000000 UZS
Company: Exadot
Stack: MySQL, PostgreSQL, Django, ORM, REST API
Category: Backend
Programming language: Python, SQL
`

Объяснение:
`
Тут не сказано напрямую, но мы можем узнать по требованию MySQL и PostgreSQL, что требуется язык SQL.
`

--------------------------------------------------------------

Текст сообщения:
`
"Najot Ta'lim" markazi Farg'ona filiali uchun "NodeJS" yo'nalishi bo'yicha mentor izlamoqda!

Maosh: 3 mln so'mdan 7 mln so'mgacha

Talablar:
- NodeJS da eng kamida 2 yillik tajriba;
- Express.js, Nest.js da amaliy ish tajribasi;
- Database (PostgreSQL, MongoDB) ni yaxshi bilish;
`

Ответ:
`
Position: JavaScript FullStack
Experience: 1-3 Years
Location: Ferghana region
Salary: from 3000000 to 7000000 UZS
Company: Najot Ta'lim
Stack: Node.js, Express.js,  NestJS, PostgreSQL, MongoDB
Category: FullStack
Programming language: JavaScript, SQL, NoSQL
`

Объяснение:
`
У нас нету категори учителей или менторов. От позиции просят универсальные знания и поэтому мы сделаем его категорию FullStack. Раз категория FullStack и требуется JavaScript, то позиция будет JavaScript FullStack.
`

--------------------------------------------------------------

Текст сообщения: {context}
Ответ:
"""
