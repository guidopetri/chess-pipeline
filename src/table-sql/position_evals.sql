create table position_evals(
id            serial   primary key,
fen           text     not null,
evaluation    real     not null,
eval_depth    smallint not null
);
