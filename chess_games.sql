create table chess_games(
    id                serial  primary key,
    black             text    not null,
    black_elo         real    not null,
    black_rating_diff real    not null,
    date_played       date    not null,
    opening_played    text    not null,
    event_type        text    not null,
    result            text    not null,
    round             text,
    game_link         text    not null,
    termination       text    not null,
    time_control      text    not null,
    utc_date_played   date    not null,
    time_played       time    not null,
    chess_variant     text    not null,
    white             text    not null,
    white_elo         real    not null,
    white_rating_diff real    not null
);