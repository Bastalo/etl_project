import logging
import sys
from pathlib import Path

# Настройка логирования
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler = logging.FileHandler('hh_search.log', mode='a', encoding='utf8')
handler.setFormatter(formatter)
logging.basicConfig(level=logging.DEBUG, handlers=[handler])
logger = logging.getLogger('hh_search')
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
logging.getLogger('urllib3').propagate = False

import tenacity
import hydra
from hydra import compose, initialize
import requests
from bs4 import BeautifulSoup
import json
from urllib.parse import urlencode
import polars as pl
import pendulum
from sqlalchemy import create_engine, text
from tqdm.auto import tqdm

hydra.initialize(version_base=None, config_path='conf')
conf = hydra.compose(config_name='config')

TELEGRAM_TOKEN = conf.telegram.token
TELEGRAM_CHAT_ID = conf.telegram.chat_id

def send_telegram_message(text: str):
    """Отправляет текстовое сообщение в Telegram"""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram token or chat_id not set, message not sent")
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        data = {
            'chat_id': TELEGRAM_CHAT_ID,
            'text': text,
            'parse_mode': 'HTML'
        }
        r = requests.post(url, data=data, timeout=10)
        r.raise_for_status()
        logger.info("Telegram message sent successfully")
    except Exception as e:
        logger.error(f"Failed to send Telegram message: {e}")

def my_before_sleep(retry_state):
    # эта функция будет запускаться после неудачи (кроме последней) перед ожиданием
    # upcoming_sleep заполняется только на этом этапе, не в after
    logging.warning(f'Attempt #{retry_state.attempt_number} failed! Error: {retry_state.outcome.exception()} Going to retry. Sleep {retry_state.upcoming_sleep} seconds')

def my_before(retry_state):
    # эта функция запускается сразу перед новой попыткой
    # первую попытку не логируем
    if retry_state.attempt_number >= 2:
        logging.warning(f'Starting attempt #{retry_state.attempt_number}')

def on_last_fail(retry_state):
    # в случае окончательной ошибки пишем в лог ERROR и возвращаем Exception
    # логику можно менять в зависимости от логики самого приложения
    logging.error(f'Attempt #{retry_state.attempt_number} failed! Return Exception. Last error: {retry_state.outcome.exception()}')
    return retry_state.outcome.exception()

my_retry = tenacity.retry(
    stop=tenacity.stop_after_attempt(5),
    wait=tenacity.wait_fixed(1),
    before=my_before,
    before_sleep=my_before_sleep,
    retry_error_callback=on_last_fail, # если ничего не удалось, возвращаем просто exception
    reraise=False # при True возвращает оригинальную ошибку, при False - RetryError
)

@my_retry
def fetch_data(url: str) -> requests.Response:
    """Выполняет HTTP-запрос с повторными попытками при ошибках"""
    r = requests.get(
        url,
        headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/13'
        },
        timeout=30
    )
    r.raise_for_status()
    return r

def main():
    start_time = pendulum.now('UTC')
    total_vacancies = 0
    pages_processed = 0
    error_occurred = False
    error_msg = ""

    try:
        db = create_engine(conf.database.url)

        # Получение максимальной даты запроса
        with db.connect() as con:
            result = con.execute(text("SELECT MAX(request_dttm) AS hwm FROM etl.hh_search")).fetchone()
            hwm = result[0] if result else None

        if hwm is None:
            date_from = '01.01.2026 00:00:00'
        else:
            date_from = pendulum.instance(hwm).format("DD.MM.YYYY HH:mm:ss")

        logger.info(f"Searching vacancies from {date_from}")

        # Параметры запроса
        q_base = {
            'area': conf.hh_api.area,
            'text': conf.hh_api.text,
            'search_field': conf.hh_api.search_field,
            'date_from': date_from,
            'no_magic': 'true',
            'items_on_page': conf.hh_api.get('items_on_page', 20),
            'order_by': 'publication_time',
            'enable_snippets': 'true'
        }

        # Первый запрос для определения количества страниц
        q_first = q_base.copy()
        q_first['page'] = 0
        url_first = f"https://hh.ru/search/vacancy?{urlencode(q_first)}"

        r = fetch_data(url_first)
        soup = BeautifulSoup(r.content, 'html.parser')
        d = json.loads(soup.select('template#HH-Lux-InitialState')[0].text)

        if d['vacancySearchResult']['paging'] is None:
            logger.info('Нет новых вакансий')
            pages_count = 0
        else:
            if d['vacancySearchResult']['paging']['lastPage'] is None:
                pages_count = d['vacancySearchResult']['paging']['pages'][-1]['page'] + 1
            else:
                pages_count = d['vacancySearchResult']['paging']['lastPage']['page'] + 1

        # Обработка страниц
        for page_idx in tqdm(range(pages_count), desc="Processing pages"):
            logger.info(f"Processing page {page_idx}")
            q_page = q_base.copy()
            q_page['page'] = page_idx
            url_page = f"https://hh.ru/search/vacancy?{urlencode(q_page)}"

            now = pendulum.now('UTC')
            r_page = fetch_data(url_page)
            soup_page = BeautifulSoup(r_page.content, 'html.parser')
            d_page = json.loads(soup_page.select('template#HH-Lux-InitialState')[0].text)

            rows = []
            for item in d_page['vacancySearchResult']['vacancies']:
                row = {
                    'request_dttm': now,
                    'vacancy_id': item['vacancyId'],
                    'vacancy_title': item['name'],
                    'company_id': item['company']['id'],
                    'company_title': item['company']['name'],
                    'company_visible_name': item['company']['visibleName'],
                    'publication_time': pendulum.parse(item['publicationTime']['$']),
                    'last_change_time': pendulum.parse(item['lastChangeTime']['$']),
                    'creation_time': pendulum.parse(item['creationTime']),
                    'is_adv': item.get('@isAdv', 'false'),
                    'snippet': json.dumps(item['snippet'], ensure_ascii=False),
                    'responses_count': item['responsesCount'],
                    'total_responses_count': item['totalResponsesCount']
                }
                rows.append(row)

            if rows:
                df = pl.DataFrame(rows)
                df.write_database('etl.hh_search', db, if_table_exists='append')
                total_vacancies += len(rows)
                pages_processed += 1

        end_time = pendulum.now('UTC')
        duration = (end_time - start_time).in_words(locale='ru')

        message = (
            f"<b>Результаты поиска вакансий</b>\n"
            f"Начало поиска: {start_time}\n"
            f"Обработано страниц: {pages_processed}\n"
            f"Новых вакансий: {total_vacancies}\n"
            f"Время выполнения: {duration}"
        )

    except Exception as e:
        logger.exception("Critical error in main script")
        error_occurred = True
        error_msg = str(e)
        message = f"<b>Ошибка при выполнении скрипта</b>\n\n{error_msg}"
    finally:
        send_telegram_message(message)

        # Если была ошибка, завершаемся с ненулевым кодом
        if error_occurred:
            sys.exit(1)
        else:
            sys.exit(0)

if __name__ == '__main__':
    main()
