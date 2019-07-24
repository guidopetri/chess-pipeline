create table castling(
    id            serial unique not null,
    game          text          not null,
    side          text          not null,
    castling_side text          not null,
    primary key (game, side)
);