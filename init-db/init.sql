CREATE TABLE IF NOT EXISTS Cars (
	id serial PRIMARY KEY,
	mark text NOT NULL,
	model text NOT NULL,
	engine_volume numeric NOT NULL,
	published_at integer NOT NULL
)