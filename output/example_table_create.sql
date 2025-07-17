CREATE TABLE main.gold.users (
  id BIGINT NOT NULL COMMENT 'Primary key for users table',
  username STRING NOT NULL COMMENT 'Unique username for the user',
  email STRING NOT NULL COMMENT 'Users email address',
  age INT COMMENT 'Users age in years',
  created_at TIMESTAMP NOT NULL COMMENT 'When the user account was created',
  is_active BOOLEAN NOT NULL COMMENT 'Whether the user account is active'
);