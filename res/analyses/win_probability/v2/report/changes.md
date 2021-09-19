# Main changes

This analysis is mostly documented [here](https://github.com/charlesoblack/chess-pipeline/issues/40).

The main change in this model compared to v1 is that clock times are being considered, both for white/black sides as well as the presence of increment.

# Motivation

Since most of the data in the database is for short-time-control games, especially games without increment, having a good position on the board doesn't necessarily mean a player will win the game. The purpose of adding in clock times, as well as the presence of increment, is to attempt to capture that relationship.
