# Justfile for KRX Auto Crawling (Windows)

# Load .env file
set dotenv-load

# Use PowerShell on Windows
set shell := ["powershell", "-c"]

import 'Justfile.common'
