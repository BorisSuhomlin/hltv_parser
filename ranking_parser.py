from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time
import pandas as pd
import cloudscraper

def get_unique_countries(url):
    try:
        scraper = cloudscraper.create_scraper()
        response = scraper.get(url)
        if response.status_code != 200:
            print(f"Страница {url} недоступна (код: {response.status_code})")
            return None
        soup = BeautifulSoup(response.text, "html.parser")
        countries = soup.find_all("div", class_="ranking-country")
        country_names = [country.text.strip() for country in countries]
        unique_countries = set(country_names)  # Удаляем дубликаты
        return len(unique_countries)
    except Exception as e:
        print(f"Ошибка при обработке {url}: {e}")
        return None

# Генерация дат с 2015 года до текущей даты
start_date = datetime(2015, 10, 5)  # Начальная дата (можно уточнить на сайте)
current_date = datetime.now()
dates = []
current = start_date
while current <= current_date:
    dates.append(current)
    current += timedelta(days=7)  # Шаг в 1 неделю

# Форматирование дат для URL
date_strings = [date.strftime("%Y/%B") + "/" + str(date.day) for date in dates]
date_strings = [ds.lower() for ds in date_strings]  # Приводим к нижнему регистру

# Создание списка URL
urls = [f"https://www.hltv.org/ranking/teams/{ds}" for ds in date_strings]

# Сбор данных
results = {}
for url in urls:
    date_str = "/".join(url.split("/")[-3:])  # Например, "2015/january/1"
    num_countries = get_unique_countries(url)
    if num_countries is not None:
        results[date_str] = num_countries
    time.sleep(1)  # Задержка в 1 секунду между запросами

# Сохранение результатов в CSV
df = pd.DataFrame(list(results.items()), columns=["Date", "Unique Countries"])
df.to_csv("hltv_unique_countries.csv", index=False)

print("Парсинг завершен. Результаты сохранены в hltv_unique_countries.csv")