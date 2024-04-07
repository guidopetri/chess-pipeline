begin;
delete from game_evals where game_link in (
    select distinct game_link from game_evals where evaluation = 'NaN'
);
commit;