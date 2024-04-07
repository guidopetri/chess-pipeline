begin;
alter table chess_games add column has_promotion boolean;
alter table chess_games add column promotion_count_white smallint;
alter table chess_games add column promotion_count_black smallint;
alter table chess_games add column promotions_white text;
alter table chess_games add column promotions_black text;

alter table chess_games alter column has_promotion set not null;
alter table chess_games alter column promotion_count_white set not null;
alter table chess_games alter column promotion_count_black set not null;
commit;
