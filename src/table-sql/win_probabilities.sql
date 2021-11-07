create table win_probabilities(
id                      serial   primary key,
game_link               text     not null,
half_move               smallint not null,
win_probability_white   real     not null,
win_prob_model_version  text     not null
);
