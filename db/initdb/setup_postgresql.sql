-- this file will set up the entire structure for chess_db

-- overview:
-- create the main database and the task history database
-- create the users for luigi, read access and task history
-- grant privileges to respective users

-- create chess_db
create database chess_db;
\c chess_db

-- create task_history_db
create database task_history_db;

-- make sure you have this file in the folder you're in, or change the location
\i /sql_scripts/tables/chess_games.sql
\i /sql_scripts/tables/eco_codes.sql
\i /sql_scripts/tables/game_clocks.sql
\i /sql_scripts/tables/game_moves.sql
\i /sql_scripts/tables/game_materials.sql
\i /sql_scripts/tables/game_positions.sql
\i /sql_scripts/tables/position_evals.sql
\i /sql_scripts/tables/win_probabilities.sql
\i /sql_scripts/tables/win_probabilities_eval_only.sql
\i /sql_scripts/tables/game_evals_view.sql

--grant luigi user privileges on database
alter default privileges in schema public grant select, insert, update, delete, truncate on tables to luigi_user;
alter default privileges in schema public grant usage on sequences to luigi_user;
grant select, insert, update, delete, truncate on all tables in schema public to luigi_user;
grant usage on all sequences in schema public to luigi_user;

-- grant task history user privileges on task history db
grant create, connect on database task_history_db to task_history_user;

-- read-only role
create role read_access;

-- grant access to existing tables
grant usage on schema public to read_access;
grant select on all tables in schema public to read_access;

-- grant access to future tables
alter default privileges in schema public grant select on tables to read_access;

grant read_access to read_user;

\i /sql_scripts/assorted_sql/copy_eco_codes.sql
\i /sql_scripts/assorted_sql/copy_win_probabilities.sql
