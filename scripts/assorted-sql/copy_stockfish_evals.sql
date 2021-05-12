begin;
create temp table new_evals (like position_evals);
alter table new_evals alter column eval_depth drop not null;
alter table new_evals alter column id drop not null;

copy new_evals(fen, evaluation) from '../res/content/new_stockfish_evals.csv' with csv delimiter ',';

update position_evals set evaluation = new_evals.evaluation, eval_depth = 20
    from new_evals
    where new_evals.fen = position_evals.fen;

commit;
