# Justfile for KRX Auto Crawling (Windows)

# Load .env file
set dotenv-load

# Use PowerShell on Windows
set shell := ["powershell", "-c"]

# Default docker-compose command for Windows
docker_compose := "docker-compose"

import 'Justfile.common'
