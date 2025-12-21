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
- MIT: инкрементальный курсорный обход. Для разовой полной перепроверки включите `MIT_FULL_SCAN=1`, затем верните `0`.
- VRI: расписание догрузки (ежечасно/ежедневно/еженедельно/ежемесячно) через переменные `VRI_*`.
- Перенос в prod: раз в час (или по `TRANSFER_EVERY_HOURS`), только если текущий запуск прошел без ошибок.

## Структура
- `backend_sync.py` — основной загрузчик MIT/VRI + перенос в prod.
- `fgis_clickhouse/fgis_api.py` — обращения к API ФГИС.
- `fgis_clickhouse/http_client.py` — HTTP-клиент с ограничением RPS и ретраями.
- `fgis_clickhouse/clickhouse_io.py` — ClickHouse I/O и DDL.
- `fgis_clickhouse/utils.py` — вспомогательные функции.
