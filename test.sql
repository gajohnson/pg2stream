DROP TABLE IF EXISTS pg2stream_test_tbl;

CREATE TABLE pg2stream_test_tbl (
	id serial primary key,
	name varchar(255),
	value int,
	updated_at timestamp
);

INSERT INTO pg2stream_test_tbl (id, name, value, updated_at)
SELECT generate_series(1, 1000), 'a', 1, NOW();

UPDATE pg2stream_test_tbl SET name = 'b';
