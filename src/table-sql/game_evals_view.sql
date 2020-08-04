create view game_evals as
    select game_link,
           half_move,
           game_positions.fen,
           evaluation,
           eval_depth,
           probability_lr      as win_probability_lr,
           probability_bayes   as win_probability_bayes
    from game_positions
    inner join position_evals      on position_evals.fen = game_positions.fen
    inner join win_probabilities   on win_probabilities.eval = position_evals.evaluation
;
