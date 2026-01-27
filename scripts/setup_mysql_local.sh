#!/bin/bash
# Setup MySQL database for a local node
# Usage: ./scripts/setup_mysql_local.sh <node_id> [mysql_root_password]
# Example: ./scripts/setup_mysql_local.sh 1 root_password_123
#
# This script will:
# 1. Create the database for the specified node
# 2. Create the ddb_user with permissions
# 3. Initialize the schema and sample data

set -e

NODE_ID=${1:-1}
MYSQL_ROOT_PASSWORD=${2:-"root_password_123"}
MYSQL_USER=${MYSQL_USER:-"ddb_user"}
MYSQL_PASSWORD=${MYSQL_PASSWORD:-"ddb_password"}

if [ -z "$1" ]; then
    echo "Usage: $0 <node_id> [mysql_root_password]"
    echo "Example: $0 1 my_root_password"
    exit 1
fi

DATABASE_NAME="ddb_node${NODE_ID}"
AUTO_INCREMENT_OFFSET=$NODE_ID

echo "Setting up MySQL for Node $NODE_ID..."
echo "Database: $DATABASE_NAME"
echo "User: $MYSQL_USER"

# Create database and user
mysql -u root -p"$MYSQL_ROOT_PASSWORD" <<EOF
CREATE DATABASE IF NOT EXISTS $DATABASE_NAME;

CREATE USER IF NOT EXISTS '$MYSQL_USER'@'localhost' IDENTIFIED BY '$MYSQL_PASSWORD';
CREATE USER IF NOT EXISTS '$MYSQL_USER'@'%' IDENTIFIED BY '$MYSQL_PASSWORD';

GRANT ALL PRIVILEGES ON $DATABASE_NAME.* TO '$MYSQL_USER'@'localhost';
GRANT ALL PRIVILEGES ON $DATABASE_NAME.* TO '$MYSQL_USER'@'%';
FLUSH PRIVILEGES;

USE $DATABASE_NAME;

-- Set auto-increment offset for this node
SET @@auto_increment_increment = 3;
SET @@auto_increment_offset = $AUTO_INCREMENT_OFFSET;

-- Create users table
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_email (email),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Create transactions_log table
CREATE TABLE IF NOT EXISTS transactions_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    transaction_id VARCHAR(255) NOT NULL,
    query_type VARCHAR(50) NOT NULL,
    query_text TEXT NOT NULL,
    status VARCHAR(50) NOT NULL,
    node_id INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_transaction_id (transaction_id),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Insert sample data only if table is empty
INSERT INTO users (name, email)
SELECT * FROM (SELECT 'Alice Johnson', 'alice@example.com') AS tmp
WHERE NOT EXISTS (SELECT 1 FROM users WHERE email = 'alice@example.com');

INSERT INTO users (name, email)
SELECT * FROM (SELECT 'Bob Smith', 'bob@example.com') AS tmp
WHERE NOT EXISTS (SELECT 1 FROM users WHERE email = 'bob@example.com');

INSERT INTO users (name, email)
SELECT * FROM (SELECT 'Carol Williams', 'carol@example.com') AS tmp
WHERE NOT EXISTS (SELECT 1 FROM users WHERE email = 'carol@example.com');
EOF

echo "MySQL setup complete for Node $NODE_ID!"
echo "Database '$DATABASE_NAME' is ready."
