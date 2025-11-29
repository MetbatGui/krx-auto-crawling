# Justfile for KRX Auto Crawling

# Load .env file
set dotenv-load

# Default: Show help
default:
    @just --list

# --- Docker Commands ---

# Build the Docker image
build:
    docker-compose -f docker/docker-compose.yml build

# Run crawl command in Docker
# Usage: just crawl [args]
# Example: just crawl 20251130 --drive
crawl +args:
    docker-compose -f docker/docker-compose.yml run --rm netbuy crawl {{args}}

# Run download command in Docker
# Usage: just download [args]
# Example: just download 20251130
download +args:
    docker-compose -f docker/docker-compose.yml run --rm netbuy download {{args}}

# Run arbitrary command in Docker
# Usage: just run [command]
# Example: just run netbuy --help
run +args:
    docker-compose -f docker/docker-compose.yml run --rm {{args}}

# Clean up Docker resources (containers, networks)
clean:
    docker-compose -f docker/docker-compose.yml down
