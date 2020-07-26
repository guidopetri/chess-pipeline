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
\i ../src/table-sql/chess_games.sql
\i ../src/table-sql/eco_codes.sql
\i ../src/table-sql/game_clocks.sql
\i ../src/table-sql/game_evals.sql
\i ../src/table-sql/game_moves.sql

-- create the user that luigi will be using
create user luigi_user;
\password luigi_user

--grant luigi user privileges on database
alter default privileges in schema public grant select, insert, update, delete, truncate on tables to luigi_user;
alter default privileges in schema public grant usage on sequences to luigi_user;
grant select, insert, update, delete, truncate on all tables in schema public to luigi_user;
grant usage on all sequences in schema public to luigi_user;

-- the task history user
create user task_history_user;
\password task_history_user

-- grant task history user privileges on task history db
grant create, connect on database task_history_db to task_history_user;

-- a read-only user
create user read_user;
\password read_user

-- read-only role
create role read_access;

-- grant access to existing tables
grant usage on schema public to read_access;
grant select on all tables in schema public to read_access;

-- grant access to future tables
alter default privileges in schema public grant select on tables to read_access;

grant read_access to read_user;

\i assorted-sql/copy_eco_codes.sql
