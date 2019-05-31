# cdrloadtest

insert_cdr_pg.pl - Simulate loading CDR's into a PostgreSQL database using array inserts. Either one table per hour, or one partition per hour (range partitioning on PostgreSQL 10.x+) are supported. The number of rows per array insert, and the number of workers, can be customised in the source code.

select_random_pg.pl - Simulate querying random MSISDN's (mobile phone numbers) to mimic users searching for specific customer transactions.

Connect parameters (hostname, database, user name, password) are all *hard coded* in the source code. Change as necessary.

Requires Perl DBI, DBD::PostgreSQL
