# FGIS Arshin

## Описание
Минимальный загрузчик ФГИС «Аршин» → ClickHouse. Скрипт скачивает утверждения типа (MIT) и поверки (VRI) в `fgis_test` и по расписанию переносит данные в `fgis_prod`.

## Стек
`Python`, `requests`, `clickhouse-driver`, `python-dotenv`

## Запуск
1. `pip install -r requirements.txt`
2. Скопируйте `example.env` → `.env` и заполните параметры.
3. Запуск: `python backend_sync.py` (лучше через cron/systemd).

## Логика синхронизации
- MIT: курсорный обход каталога. По умолчанию безопасно проходит весь курсор и не останавливается на первой странице без новых MIT. Для настройки ранней остановки используйте `MIT_STOP_AFTER_EMPTY_PAGES` (`0` = выключено, самый безопасный дефолт).
- `MIT_FULL_SCAN` сохранен для обратной совместимости. Если нужен максимально консервативный режим, оставляйте `MIT_STOP_AFTER_EMPTY_PAGES=0`.
- Для медленных ответов FGIS можно увеличить `HTTP_TIMEOUT` и временно уменьшить `MIT_ROWS` для тестового прогона.
- VRI: расписание догрузки (ежечасно/ежедневно/еженедельно/ежемесячно) через переменные `VRI_*`.
- Перенос в prod: раз в час (или по `TRANSFER_EVERY_HOURS`), только если текущий запуск прошел без ошибок.

## Структура
- `backend_sync.py` — основной загрузчик MIT/VRI + перенос в prod.
- `fgis_clickhouse/fgis_api.py` — обращения к API ФГИС.
- `fgis_clickhouse/http_client.py` — HTTP-клиент с ограничением RPS и ретраями.
- `fgis_clickhouse/clickhouse_io.py` — ClickHouse I/O и DDL.
- `fgis_clickhouse/utils.py` — вспомогательные функции.
