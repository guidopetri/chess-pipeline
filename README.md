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

## Running the script

To run the script, run the following command with the location of `chess_pipeline.py` in your PYTHONPATH:

`luigi --module chess_pipeline CopyGames`

with the following arguments:

- `--user`, the PostGreSQL user to log in as
- `--password`, the password for the above user
- `--host`, the hostname for where the server is running
- `--port`, the port to which the server is listening
- `--database`, the database to write to

By default, the script pulls Blitz games from the last two days for the user [`thibault`](http://lichess.org/@/thibault). This can be changed by passing in the following arguments:

- `--player`, for the chess player
- `--perf-type`, for the kind of chess game - bullet, blitz, classical, etc.
- `--since`, for since when to pull. This is given in Unix time.
- `--single-day`, if only a single day is to be pulled from the API (this is a boolean flag)

Optional arguments:

- `--lichess-token` is the lichess API token to be used for faster API calls

The above can also be added to your `crontab`, as long as your PATH and PYTHONPATH are set up correctly.

The script defaults to writing to a table called `chess_games`.

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

and similar columns for opponent.

## TODO

**bold**: currently working on it
*italic*: in testing

- Add signifier for tentative rating or not (is this even available?)
- column for whether berserked or not in arena game
- More stats using python-chess `Visitors`
- Clean up column names
- ~Use Lichess API directly (both JSON and PGN formats)?~
- Add some of the Tableau graphs to the repo
