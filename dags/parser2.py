from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from time import localtime, strftime
import requests
import psycopg2
from bs4 import BeautifulSoup
import datetime  # Импортируем модуль для работы с датой и временем

default_args = {
    "owner": "airflow",
    'start_date': days_ago(1)
}
db_params = {
    "dbname": "test",
    "user": "postgres",
    "password": "password",
    "host": "host.docker.internal",
    "port": 5430
}

def create_tb_postgres():
    # Установка соединения с PostgreSQL
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    cur.execute("""CREATE TABLE IF NOT EXISTS public.vacancies (link_vacancy VARCHAR(500) PRIMARY KEY, grade VARCHAR(50), spesialization VARCHAR(150),
                                             company VARCHAR(150), vacancy VARCHAR(200), 
                                             skills VARCHAR(1000), meta VARCHAR(200),salary VARCHAR(100), 
                                             date VARCHAR(150));""")
        
    cur.execute("""CREATE TABLE IF NOT EXISTS public.vacancy_today (link_vacancy VARCHAR(500) PRIMARY KEY,
                                             grade VARCHAR(50), spesialization VARCHAR(150),
                                             company VARCHAR(150), vacancy VARCHAR(200), 
                                             salary_min INTEGER, salary_max INTEGER,
                                             date VARCHAR(150));""")
        
    cur.execute("""CREATE TABLE IF NOT EXISTS public.vacancy_history (link_vacancy VARCHAR(500) PRIMARY KEY,
                                             grade VARCHAR(50), spesialization VARCHAR(150),
                                             company VARCHAR(150), vacancy VARCHAR(200), 
                                             salary_min INTEGER, salary_max INTEGER,
                                             date VARCHAR(150));""")

    cur.execute("""CREATE TABLE IF NOT EXISTS public.s_meta (id serial PRIMARY KEY, meta VARCHAR(150) UNIQUE);""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS public.s_skills (id serial PRIMARY KEY, skill VARCHAR(50) UNIQUE);""")
        
    cur.execute("""CREATE TABLE IF NOT EXISTS public.vacancy_skills (link_vacancy VARCHAR(500), 
        skill_id integer, PRIMARY KEY (link_vacancy, skill_id));""")
        
    cur.execute("""CREATE TABLE IF NOT EXISTS public.vacancy_meta (link_vacancy VARCHAR(500), 
        meta_id integer, PRIMARY KEY (link_vacancy, meta_id));""")
    conn.commit()
      

def import_vacances_habr ():
  BASE_URL = "https://career.habr.com/vacancies"
  HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:98.0) Gecko/20100101 Firefox/98.0",
}

  def get_html(url, params={}):
    r = requests.get(url, headers=HEADERS, params=params)
    return r

  def get_vacancy(html):
    soup = BeautifulSoup(html, "html.parser")
    items = soup.find_all("div", class_="vacancy-card")

    vacancy = []

    for item in items:
        company = item.find("div", class_="vacancy-card__company-title").get_text(strip=True)
        vacancy_title = item.find("a", class_="vacancy-card__title-link").text.strip()
        skills = item.find("div", class_="vacancy-card__skills").text.strip()
        meta = item.find("div", class_="vacancy-card__meta").text.strip()
        salary = item.find("div", class_="basic-salary").text.strip()
        date = item.find("time", class_="basic-date").text.strip()
        link_vacancy = "https://career.habr.com" + item.find("a", class_="vacancy-card__title-link")["href"]

        vacancy.append({
            "company": company,
            "vacancy": vacancy_title,
            "skills": skills,
            "meta": meta,
            "salary": salary,
            "date": date,
            "link_vacancy": link_vacancy
        })

    return vacancy

  def get_qid_description(qid):
    qid_descriptions = {
        1: "Intern",
        3: "Junior",
        4: "Middle",
        5: "Senior",
        6: "Lead"
    }
    return qid_descriptions.get(qid, "Unknown")

  def get_s_value_description(s_value):
    s_value_descriptions = {
        41: "Системный аналитик",
        42: "Бизнес аналитик",
        43: "Аналитик данных",
        95: "BI разработчик",
        97: "Продуктовый аналитик",
        98: "Аналитик мобильных приложений",
        99: "Гейм аналитик",
        100: "WEB аналитик"
    }
    return s_value_descriptions.get(s_value, "Unknown")

  def save_vacancy_to_postgres(items, db_params, qid, s_value):
    qid_description = get_qid_description(qid)
    s_value_description = get_s_value_description(s_value)

    # Установка соединения с PostgreSQL
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    for item in items:
        company = item["company"]
        vacancy_title = item["vacancy"]
        skills = item["skills"]
        meta = item["meta"]
        salary = item["salary"]
        date = item["date"]
        link_vacancy = item["link_vacancy"]

        sql = """INSERT INTO public.vacancies (grade, spesialization, company, vacancy, 
                                               skills, meta, salary, date, link_vacancy)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                 ON CONFLICT (link_vacancy) DO NOTHING;"""
        values = (qid_description, s_value_description, company, vacancy_title, skills, meta, salary, date, link_vacancy)
        cur.execute(sql, values)
        conn.commit()
  
  def get_total_pages(html):
    soup = BeautifulSoup(html, "html.parser")
    pagination = soup.find("div", class_="pagination")
    if pagination:
        pages = pagination.find_all("a")
        if pages:
            last_page = pages[-2].text
            return int(last_page)
    return 1

  def parser():
    qid_values = [1, 3, 4, 5, 6]
    s_values = [41, 42, 43, 95, 97, 98, 99, 100]

    for qid in qid_values:
        for s_value in s_values:
            url = f"{BASE_URL}?qid={qid}&s[]={s_value}&type=all"
            html = get_html(url)
            if html.status_code == 200:
                total_pages = get_total_pages(html.text)
                print(f"Found {total_pages} pages for qid={qid}, s[]={s_value}")
                for page in range(1, total_pages + 1):
                    url = f"{BASE_URL}?qid={qid}&s[]={s_value}&type=all&page={page}"
                    html = get_html(url)
                    if html.status_code == 200:
                        vacancy = get_vacancy(html.text)
                        save_vacancy_to_postgres(vacancy, db_params, qid, s_value)
                        print(f"Parsing completed for qid={qid}, s[]={s_value}, page {page}")
                    else:
                        print(f"Error parsing for qid={qid}, s[]={s_value}, page {page}")
            else:
                print(f"Error getting page for qid={qid}, s[]={s_value}")

  parser()
        
def transform_insert_core():
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()       
    cur.execute("""DELETE
                FROM public.vacancy_today;""")
    cur.execute("""INSERT INTO public.vacancy_today (link_vacancy, grade, spesialization, company, vacancy, date,  salary_min, salary_max)
    SELECT
    v.link_vacancy, v.grade, v.spesialization, v.company, v.vacancy, v.date, 
    CASE
        WHEN POSITION('от' IN v.salary) = 1 AND POSITION('до' IN v.salary) > 0
        THEN CASE
            WHEN REGEXP_REPLACE(SUBSTRING(v.salary FROM 4 FOR POSITION('до' IN v.salary) - 4), E'[^0-9]', '', 'g') <> ''
            THEN CAST(REGEXP_REPLACE(SUBSTRING(v.salary FROM 4 FOR POSITION('до' IN v.salary) - 4), E'[^0-9]', '', 'g') AS integer)*100
            ELSE NULL
            END
        WHEN POSITION('от' IN v.salary) > 0
        THEN CASE
            WHEN REGEXP_REPLACE(SUBSTRING(v.salary FROM 4), E'[^0-9]', '', 'g') <> ''
            THEN CAST(REGEXP_REPLACE(SUBSTRING(v.salary FROM 4), E'[^0-9]', '', 'g') AS integer)*100
            ELSE NULL
            END
        ELSE NULL
    END AS salary_min,
    CASE
        WHEN POSITION('до' IN v.salary) > 0
        THEN CASE
            WHEN REGEXP_REPLACE(SUBSTRING(v.salary FROM POSITION('до' IN v.salary) + 3), E'[^0-9]', '', 'g') <> ''
            THEN CAST(REGEXP_REPLACE(SUBSTRING(v.salary FROM POSITION('до' IN v.salary) + 3), E'[^0-9]', '', 'g') AS integer)*100
            ELSE NULL
            END
        ELSE NULL
    END AS salary_max
    FROM vacancies v
    where v.salary like '%$%'
    ON CONFLICT (link_vacancy) DO NOTHING;""")
    cur.execute("""INSERT INTO public.vacancy_today (link_vacancy, grade, spesialization, company, vacancy, date,   salary_min, salary_max)
    SELECT
    v.link_vacancy, v.grade, v.spesialization, v.company, v.vacancy, v.date,  
    CASE
        WHEN POSITION('от' IN v.salary) = 1 AND POSITION('до' IN v.salary) > 0
        THEN CASE
            WHEN REGEXP_REPLACE(SUBSTRING(v.salary FROM 4 FOR POSITION('до' IN v.salary) - 4), E'[^0-9]', '', 'g') <> ''
            THEN CAST(REGEXP_REPLACE(SUBSTRING(v.salary FROM 4 FOR POSITION('до' IN v.salary) - 4), E'[^0-9]', '', 'g') AS integer)
            ELSE NULL
            END
        WHEN POSITION('от' IN v.salary) > 0
        THEN CASE
            WHEN REGEXP_REPLACE(SUBSTRING(v.salary FROM 4), E'[^0-9]', '', 'g') <> ''
            THEN CAST(REGEXP_REPLACE(SUBSTRING(v.salary FROM 4), E'[^0-9]', '', 'g') AS integer)
            ELSE NULL
            END
        ELSE NULL
    END AS salary_min,
    CASE
        WHEN POSITION('до' IN v.salary) > 0
        THEN CASE
            WHEN REGEXP_REPLACE(SUBSTRING(v.salary FROM POSITION('до' IN v.salary) + 3), E'[^0-9]', '', 'g') <> ''
            THEN CAST(REGEXP_REPLACE(SUBSTRING(v.salary FROM POSITION('до' IN v.salary) + 3), E'[^0-9]', '', 'g') AS integer)
            ELSE NULL
            END
        ELSE NULL
    END AS salary_max
    FROM vacancies v
    where v.salary like '%₽%'
    ON CONFLICT (link_vacancy) DO NOTHING;""")
    cur.execute("""INSERT INTO public.vacancy_today (link_vacancy, grade, spesialization, company, vacancy, date)
    SELECT
    v.link_vacancy, v.grade, v.spesialization, v.company, v.vacancy, v.date
    FROM vacancies v
    where salary like ''
    ON CONFLICT (link_vacancy) DO NOTHING;""")
    cur.execute("""INSERT INTO vacancy_history (link_vacancy, grade, spesialization, company, 
                                             vacancy, date, salary_min, salary_max)
                SELECT v.link_vacancy, v.grade, v.spesialization, v.company, v.vacancy, v.date, v.salary_min, v.salary_max
                FROM vacancy_today v
                ON CONFLICT (link_vacancy) DO NOTHING;""") 
    cur.execute("""INSERT INTO public.s_skills  (skill)
    SELECT DISTINCT UNNEST(string_to_array(TRIM(BOTH ' ' FROM REGEXP_REPLACE(skills, '^[^•]*•', '', 'g')), ' • ')) AS skill
    FROM vacancies
    ON CONFLICT (skill) DO NOTHING;""")

    cur.execute("""INSERT INTO vacancy_skills (link_vacancy, skill_id)
    SELECT
    v.link_vacancy AS link_vacancy,
    s.id AS skill_id
    FROM
    vacancies AS v
    INNER JOIN
    s_skills AS s
    ON v.skills LIKE '%' || s.skill || '%'
    ON CONFLICT (link_vacancy, skill_id) DO NOTHING;""")
    cur.execute("""INSERT INTO public.s_meta  (meta)
    SELECT UNNEST(string_to_array(TRIM(BOTH ' ' FROM REGEXP_REPLACE(meta, '^[^•]*•', '', 'g')), ' • ')) AS meta
    FROM vacancies
    where meta not in ('Полный рабочий день', 'Неполный рабочий день', 'Можно удаленно')
    ON CONFLICT (meta) DO NOTHING;""")
    
    cur.execute("""INSERT INTO vacancy_meta (link_vacancy, meta_id)
    SELECT
    v.link_vacancy AS link_vacancy,
    s.id AS meta_id
    FROM
    vacancies AS v
    INNER JOIN
    s_meta AS s
    ON v.meta LIKE '%' || s.meta || '%'
    ON CONFLICT (link_vacancy, meta_id) DO NOTHING;""")
    cur.execute("""DELETE
                FROM public.vacancies;""")

    conn.commit()
    cur.close()
    conn.close()


#Определяем даг
with DAG(dag_id = "import_vacances_habr", schedule_interval = None,
    default_args = default_args, tags=["parsing", "vacances"], catchup = False) as dag:
 
    create_tb = PythonOperator(task_id = "create_tb", 
                                           python_callable = create_tb_postgres)
    import_vacances = PythonOperator(task_id = "import_vacances_habr", 
                                           python_callable = import_vacances_habr)
    core = PythonOperator(task_id = "transform_insert_core", 
                                           python_callable = transform_insert_core)
#Определяем очередность тасков      
create_tb >> import_vacances >> core

