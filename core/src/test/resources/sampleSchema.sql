CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy' , 'replication_factor': '1'};

CREATE TABLE IF NOT EXISTS test.posts (
  author_id text,
  post_id timeuuid,
  post_title text,
  PRIMARY KEY ((author_id), post_id)
);