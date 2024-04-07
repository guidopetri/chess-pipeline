begin;
insert into position_evals
    select game_evals_renamed.id + 1000000, fen, evaluation, eval_depth from game_evals_renamed
    inner join game_positions
        on game_positions.game_link = game_evals_renamed.game_link
        and game_positions.half_move = game_evals_renamed.half_move;
commit;
