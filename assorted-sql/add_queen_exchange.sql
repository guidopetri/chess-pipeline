begin;
alter table chess_games add column queen_exchanged text;
update chess_games set queen_exchanged = 'Unknown';
alter table chess_games alter column queen_exchanged set not null;
commit;