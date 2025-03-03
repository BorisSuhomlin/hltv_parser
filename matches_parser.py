import cloudscraper
from bs4 import BeautifulSoup
import time
import csv

# Создаем scraper для обхода защиты
scraper = cloudscraper.create_scraper()

# Базовый URL
base_url = "https://www.hltv.org/results?offset="

# Словарь для хранения матчей по датам
matches_per_day = {}

# Проходим по страницам (offset от 0 до 100000 с шагом 100)
for offset in range(99900, 105000, 100):
    url = base_url + str(offset)
    print(f"Обрабатываем offset={offset}")

    # Отправляем запрос
    response = scraper.get(url)
    if response.status_code == 200:
        # Парсим страницу
        soup = BeautifulSoup(response.text, 'html.parser')
        results_sublists = soup.find_all('div', class_='results-sublist')

        # Обрабатываем каждый блок с датой и матчами
        for sublist in results_sublists:
            # Извлекаем дату
            date_elem = sublist.find('span', class_='standard-headline')
            date = date_elem.text.strip() if date_elem else "Дата не найдена"

            # Подсчитываем количество матчей
            matches = sublist.find_all('div', class_='result-con')
            match_count = len(matches)

            # Обновляем словарь
            if date in matches_per_day:
                matches_per_day[date] += match_count
            else:
                matches_per_day[date] = match_count

            print(f"{date}: {match_count} матчей (итого: {matches_per_day[date]})")
    else:
        print(f"Ошибка: {response.status_code}")

    # Задержка для снижения нагрузки на сервер
    time.sleep(0.1)

# Записываем результаты в CSV
with open('matches_per_day.csv', 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['Date', 'Total Match Count'])
    for date, total in matches_per_day.items():
        writer.writerow([date, total])