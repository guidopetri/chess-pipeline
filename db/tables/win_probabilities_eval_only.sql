create table win_probabilities_eval_only(
id               serial   primary key,
eval             real     not null,
probability_lr   real     not null
);
