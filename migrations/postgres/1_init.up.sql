CREATE TABLE IF NOT EXISTS users (
  user_id varchar(255) PRIMARY KEY,
  oculus_id    varchar(255) NOT NULL,
  user_type   varchar(255) NULL
);
CREATE INDEX sessions_oculus_id_idx ON users (user_id);