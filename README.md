# FGIS Arshin

Минимальный загрузчик ФГИС «Аршин» в ClickHouse.

## Что делает `backend_sync.py`

- Один процесс за раз: второй запуск сразу выходит по lock-файлу.
- Каждые 8 часов обновляет `fgis_test`: MIT и последние 14 дней VRI.
- Раз в 7 дней сверяет `fgis_test.verifications` с ФГИС за последний год.
- Раз в 30 дней сверяет `fgis_test.verifications` с ФГИС за все время, начиная с `START_DATE`.
- Каждый день после 21:00 переносит данные из `fgis_test` в `fgis_prod`.
- Ошибочные VRI-дни в `test` перезагружаются через `DELETE + загрузка заново`, поэтому дубли вычищаются.
- VRI переносится по дням через `DELETE + INSERT ... FINAL`, поэтому дублей в `prod` не остается.
- MIT в `prod` полностью заменяется из `fgis_test.mit_registry FINAL`.

## Настройка

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
cp example.env .env
```

В `.env` обычно нужно поправить только ClickHouse-подключение и при необходимости `FGIS_RPS`.

Важно: `CH_PORT` для `backend_sync.py` должен быть native-порт ClickHouse (`9000` или `9001`), не HTTP-порт `8123`.

## Ручной запуск

```bash
cd /home/zool/projects/arshin
. .venv/bin/activate
python backend_sync.py >> run.log 2>&1
```

## User systemd timer

```bash
mkdir -p ~/.config/systemd/user
cp systemd/user/arshin-sync.service ~/.config/systemd/user/
cp systemd/user/arshin-sync.timer ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now arshin-sync.timer
```

Проверка:

```bash
systemctl --user list-timers --all | grep arshin
systemctl --user status arshin-sync.service
tail -f /home/zool/projects/arshin/run.log
```

Остановка:

```bash
systemctl --user disable --now arshin-sync.timer
systemctl --user stop arshin-sync.service
```

## Структура

- `backend_sync.py` — тонкая точка входа и совместимость со старыми импортами из ноутбуков/тестов.
- `fgis_clickhouse/pipeline.py` — общий сценарий запуска: что обновлять сейчас, когда сверять, когда публиковать в prod.
- `fgis_clickhouse/mit_sync.py` — MIT mapping, загрузка реестра и дедупликация MIT.
- `fgis_clickhouse/vri_sync.py` — VRI pagination, загрузка дней, сверка с ФГИС и сверка prod/test.
- `fgis_clickhouse/runtime.py` — расписание, `.env` helpers, `sync_state` и process lock.
- `fgis_clickhouse/dates.py` — парсинг дат и разбиение диапазонов на годы/месяцы/дни.
- `fgis_clickhouse/fgis_api.py` — HTTP endpoints ФГИС.
- `fgis_clickhouse/http_client.py` — throttling, retry и backoff.
- `fgis_clickhouse/clickhouse_io.py` — подключение и DDL ClickHouse.
