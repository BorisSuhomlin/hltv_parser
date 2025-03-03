import cloudscraper
from bs4 import BeautifulSoup
import time
import csv
from datetime import datetime

# Создаем scraper для обхода защиты
scraper = cloudscraper.create_scraper()

# Базовый URL для архива событий
base_url = "https://www.hltv.org/events/archive?offset="

# Список для хранения данных о событиях
events_data = []

for offset in range(0, 7250, 50):  # Примерный диапазон, можно изменить
    url = base_url + str(offset)
    print(f"Обрабатываем offset={offset}")

    # Настройки для повторных попыток
    max_attempts = 6  # Максимальное количество попыток
    attempt = 1
    success = False

    # Повторяем запрос, пока не будет успеха или не закончатся попытки
    while attempt <= max_attempts and not success:
        try:
            # Отправляем запрос
            response = scraper.get(url)
            if response.status_code == 200:
                # Парсим страницу
                soup = BeautifulSoup(response.text, 'html.parser')
                event_blocks = soup.find_all('a', class_='a-reset small-event standard-box')

                # Обрабатываем каждый блок события
                for event in event_blocks:
                    event_data = {}

                    # 1. Ссылка на событие
                    event_link = "https://www.hltv.org" + event['href']
                    event_data['Link'] = event_link

                    # 2. Название события
                    event_name = event.find('div', class_='text-ellipsis').text.strip()
                    event_data['Name'] = event_name

                    # 3. Количество команд
                    team_count = event.find_all('td', class_='col-value small-col')[0].text.strip()
                    event_data['Teams'] = team_count

                    # 4. Призовые
                    prize = event.find('td', class_='prizePoolEllipsis')['title'].strip()
                    if prize.startswith('$'):
                        prize = prize.replace('$', '').replace(',', '')  # Убираем $ и запятые
                    event_data['Prize'] = prize

                    # 5. Тип турнира
                    event_type = event.find_all('td', class_='col-value small-col')[-1].text.strip()
                    event_data['Type'] = event_type

                    # 6. Место проведения
                    location_elem = event.find('span', class_='smallCountry')
                    location = location_elem.find_next_sibling('span', class_='col-desc').text.strip() if location_elem else "Не указано"
                    event_data['Location'] = location[:-2]

                    # 7 и 8. Даты начала и конца
                    date_spans = event.find_all('span', {'data-time-format': 'MMM do'})
                    start_date_unix = int(date_spans[0]['data-unix']) / 1000
                    start_date = datetime.fromtimestamp(start_date_unix).strftime('%d.%m.%Y')
                    event_data['Start Date'] = start_date

                    end_date = ""
                    if len(date_spans) > 1:  # Если есть дата окончания
                        end_date_unix = int(date_spans[1]['data-unix']) / 1000
                        end_date = datetime.fromtimestamp(end_date_unix).strftime('%d.%m.%Y')
                    else:
                        end_date = start_date  # Если конца нет, используем дату начала
                    event_data['End Date'] = end_date

                    # Добавляем данные в список
                    events_data.append(event_data)

                    # print(f"Событие: {event_name} обработано")

                success = True  # Успешно обработали страницу
            else:
                print(f"Попытка {attempt}/{max_attempts} для offset={offset}: Ошибка {response.status_code}")
                attempt += 1
                time.sleep(2 ** attempt)  # Экспоненциальная задержка (2, 4, 8 сек)

        except Exception as e:
            print(f"Попытка {attempt}/{max_attempts} для offset={offset}: Исключение - {str(e)}")
            attempt += 1
            time.sleep(2)

    if not success:
        print(f"Не удалось обработать offset={offset} после {max_attempts} попыток")

    # Задержка между итерациями offset
    time.sleep(0.1)

# Записываем результаты в CSV
with open('events_archive.csv', 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['Link', 'Name', 'Teams', 'Prize', 'Type', 'Location', 'Start Date', 'End Date']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for event in events_data:
        writer.writerow(event)

print("Парсинг завершен, данные сохранены в 'events_archive.csv'")