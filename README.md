# Avito Webhook → Zenno tasks
Принимает POST от Авито (v3) по пути `/webhook/:account`, пишет задания в `/mnt/data/tasks`.
Zenno ловит новые `*.json` через FileSystemWatcher и мгновенно отвечает в веб-чате Авито.
Deploy: GitHub → Railway. Env: DEFAULT_REPLY (опц.), TASK_DIR=/mnt/data/tasks (рек.).
