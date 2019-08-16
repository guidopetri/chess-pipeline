create table game_evals(
id            serial   primary key,
game_link     text     not null,
-- we don't need 4 bytes so we may as well save space and use smallint
half_move     smallint not null,
evaluation    real     not null
);