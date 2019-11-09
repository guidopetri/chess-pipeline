create table game_moves(
id            serial   primary key,
game_link     text     not null,
-- we don't need 4 bytes so we may as well save space and use smallint
half_move     smallint not null,
move          text not null
);