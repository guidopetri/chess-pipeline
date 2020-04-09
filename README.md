# Chess games pipeline

This is a simple scripting package to pull games from [Lichess](http://lichess.org) into a PostGreSQL database.

## Requirements

In order to run it, the following Python packages are required:

- luigi
- psycopg2
- pandas
- python-chess
- python-lichess

A PostGreSQL server must also be set up properly. The data is written to a table with the format listed in `chess_games.sql`.

## Gathering chess data

To gather data from Lichess, run the following command with the location of `chess_pipeline.py` in your PYTHONPATH:

`luigi --module chess_pipeline CopyGames`

By default, the script pulls Blitz games from the last two days for the user [`thibault`](http://lichess.org/@/thibault). This can be changed by passing in the following arguments:

- `--player`, for the chess player
- `--perf-type`, for the kind of chess game - bullet, blitz, classical, etc.
- `--since`, for since when to pull. This is given in Unix time.
- `--single-day`, if only a single day is to be pulled from the API (this is a boolean flag)

The above can also be added to your `crontab`, as long as your PATH and PYTHONPATH are set up correctly.

The script defaults to writing to a (main) table called `chess_games`. It also writes to tables called `game_clocks`, `game_evals`, and `game_moves` for the clock information, the computer evaluation of the position, and the moves made in the game, respectively.

In order to access these tables, a section named `postgres_cfg` in your `luigi.cfg` file containing the following keys must exist:

- `user`, the PostGreSQL user to log in as
- `password`, the password for the above user
- `host`, the hostname for where the server is running
- `port`, the port to which the server is listening
- `database`, the database to write to

For greater control over write permissions, the following keys are used whenever data is read from the PostGreSQL server:

- `read_user`, the PostGreSQL (read-only) user to log in as
- `read_password`, the password for the above user

Optionally, you can also add a `lichess_token` section with the following key:

- `lichess-token` is the lichess API token to be used for faster API calls

### Local Stockfish

Some games don't have server-side analyses available for them. To allow analysis of these games, a local stockfish can be used to analyze each position and create an evaluation for it. This requires the `stockfish` library for conversing with stockfish. Using local analysis is as simple as adding the flag:

- `--local-stockfish`

Additionally, you also need a `stockfish_cfg` section with the keys:

- `depth`, the depth at which stockfish will analyze
- `location`, the location of the stockfish executable

Depending on the processing power of your machine, you might want to choose a low depth - analyzing all the positions takes a while. Server-side analyses are depth 20.

## Attributes

For each chess game:
  - game info (variant)
  - game result
  - game link
  - how the game finished (time forfeit/resignation)  
  - player name
  - player color
  - rating diff for player
  - player result
  - game type (time control - bullet, blitz, etc.)
  - datetime played
  - clock start time
  - clock increment
  - in arena or not
  - rated or casual
  - player rating
  - queen exchange or not
  - player castling side
  - opening name
  - opening ECO
  - move list
  - move evaluations (server-side if available, otherwise with stockfish if enabled)
  - clock times per move

and similar columns for opponent.

## Newsletter

It's also possible to send out a newsletter with some simple graphs/metrics on how your chess games have been for the week. However, there are a few additional Python package requirements for this:

- seaborn
- beautifulSoup4

Additionally, two config sections need to be added:

- `newsletter_cfg`, with a sender email under the key `sender`
- `sendgrid`, with an API key under the key `apikey`

The newsletter can be sent with the following command:

`luigi --module newsletter SendNewsletter`

with the options:

- `--player`, for the player whose games we're going to analyze
- `--receiver`, for the email address to send to

Currently, the newsletter sends out win rates (in %) for black/white in each time control category, as well as a rating progression chart by day, along with min/maxes for each day.
