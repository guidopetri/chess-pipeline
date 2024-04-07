psql -c "create user luigi_user with password '$LUIGI_USER_PASSWORD';"
psql -c "create user task_history_user with password '$TASK_HISTORY_USER_PASSWORD';"
psql -c "create user read_user with password '$READ_USER_PASSWORD';"
