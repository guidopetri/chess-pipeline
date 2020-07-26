- lichess clips at -10/10. so does pgn spy https://github.com/MGleason1/PGN-Spy . reason? apparently stockfish can't guarantee eval accuracy beyond those limits (see stockfish testing here: https://tests.stockfishchess.org/tests )

- acl isn't really a great metric in the first place. maybe winning odds could be used? there's a sigmoid function used in lichess to draw the eval graph at the bottom, see https://github.com/ornicar/lila/blob/8dcddaa1048c347cde6d0a6773515764ee71877e/ui/ceval/src/winningChances.ts

see also https://www.chessprogramming.org/Pawn_Advantage,_Win_Percentage,_and_Elo

- might be better to read up on all evaluation methods: https://www.chessprogramming.org/Evaluation is a good start

also how handicaps work: https://en.wikipedia.org/wiki/Handicap_(chess) these could be a source of inspiration

examples of "bad" acl behavior: https://lichess.org/cItbs2Uy , https://lichess.org/sSNhQb0d

also worth looking at: CAPS, the chess.com alternative to evaluations: https://www.chess.com/article/view/better-than-ratings-chess-com-s-new-caps-system

alternatively, if we want to keep ACL, we could instead just count the moves that are in the -10/10 range in the first place. this would just ignore moves that are in the extremes anyway which might make more sense. (this is testable on redash)
