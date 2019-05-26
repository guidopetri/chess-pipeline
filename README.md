# Chess games pipeline

This is a simple scripting package to pull games from [Lichess](http://lichess.org) into a PostGreSQL database.

## Requirements

In order to run it, the following Python packages are required:

- luigi
- psycopg2
- pandas

A PostGreSQL server must also be set up properly. The data is written to a table with the format listed in `chess_games.sql`.

## Running the script

To run the script, run the following command with the location of `chess_pipeline.py` in your PYTHONPATH:

`luigi --module chess_pipeline CopyGames --user <postgres_user> --password <postgres_pword> --host <hostname> --port <port> --database <db_name>`

The above can also be added to your `crontab`, as long as your PATH and PYTHONPATH are set up correctly.

The script defaults to writing to a table called `chess_games`.
