create view game_evals as
    select game_link,
           half_move,
           game_positions.fen,
           evaluation,
           eval_depth,
           probability_lr      as win_probability_lr
    from game_positions
    inner join position_evals                on position_evals.fen = game_positions.fen
    inner join win_probabilities_eval_only   on win_probabilities_eval_only.eval = position_evals.evaluation
;
