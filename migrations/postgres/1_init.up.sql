CREATE TABLE IF NOT EXISTS users (
  id serial primary key,
  user_id varchar(255)
);
CREATE INDEX id_idx ON users (user_id);