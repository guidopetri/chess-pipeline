create table game_materials(
id              serial   primary key,
game_link       text     not null,
-- we don't need 4 bytes so we may as well save space and use smallint
half_move       smallint not null,
pawns_white     smallint not null,
pawns_black     smallint not null,
bishops_white   smallint not null,
bishops_black   smallint not null,
knights_white   smallint not null,
knights_black   smallint not null,
rooks_white     smallint not null,
rooks_black     smallint not null,
queens_white    smallint not null,
queens_black    smallint not null
);
