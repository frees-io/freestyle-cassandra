
CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : '3'};

CREATE TABLE test.users (
  id uuid,
  name text,
 PRIMARY KEY (id)
);