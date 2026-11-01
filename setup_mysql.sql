-- Run once as the MySQL admin:  sudo mysql < scripts/setup_mysql.sql
-- EDIT THE PASSWORD below (and put the same one in .env).
CREATE DATABASE IF NOT EXISTS intellitx;
CREATE DATABASE IF NOT EXISTS intellitx_test;
CREATE USER IF NOT EXISTS 'intellitx'@'localhost' IDENTIFIED BY 'change_me';
CREATE USER IF NOT EXISTS 'intellitx'@'127.0.0.1' IDENTIFIED BY 'change_me';
GRANT ALL PRIVILEGES ON intellitx.*      TO 'intellitx'@'localhost';
GRANT ALL PRIVILEGES ON intellitx.*      TO 'intellitx'@'127.0.0.1';
GRANT ALL PRIVILEGES ON intellitx_test.* TO 'intellitx'@'localhost';
GRANT ALL PRIVILEGES ON intellitx_test.* TO 'intellitx'@'127.0.0.1';
-- Needed so the schema's triggers can be created while binary logging is on (MySQL 8 default):
SET GLOBAL log_bin_trust_function_creators = 1;
FLUSH PRIVILEGES;
