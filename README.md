# Introduction

You've been invited to complete this data engineering coding assignment. We have deliberately designed this assignment to be open-ended in order to leave plenty of room for you to make choices. Please document your Pyspark code.  We do not expect you to spend days on this nor to build a full-blown application.

Good luck!

-Leap.

## Assignment

In this repository you'll find a directory with parquet files containing energy meter-data.

Please build a PySpark solution that reads in the data and writes out a table showing the energy usage for each meter at each 15 minute interval, including  additional data fields flagging missing intervals, as well as the hourly averaged energy usage.  The only requirement is that the solution should only include native PySpark (and thereby no UDFs).

