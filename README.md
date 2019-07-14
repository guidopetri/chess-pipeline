# Chess games pipeline

This is a simple scripting package to pull games from [Lichess](http://lichess.org) into a PostGreSQL database.

## Requirements

In order to run it, the following Python packages are required:

- luigi
- psycopg2
- pandas
- python-chess
- python-lichess (NB: I made a PR to fix an API call that is being made improperly - this is the version that must be used)

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
- `--perfType`, for the kind of chess game - bullet, blitz, classical, etc.
- `--since`, for since when to pull. This is given in Unix time.

The above can also be added to your `crontab`, as long as your PATH and PYTHONPATH are set up correctly.

The script defaults to writing to a table called `chess_games`.