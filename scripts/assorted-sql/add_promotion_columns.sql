begin;
alter table chess_games add column has_promotion boolean;
alter table chess_games add column promotion_count smallint;
alter table chess_games add column promotions text;

alter table chess_games alter column has_promotion set not null;
alter table chess_games alter column promotion_count set not null;
commit;
