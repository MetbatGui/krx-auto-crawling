import typer
import warnings

# Suppress benign openpyxl warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

from commands.crawl import crawl
from commands.auth import auth
from commands.healthcheck import healthcheck
from commands.backfill import backfill

app = typer.Typer(help="KRX 자동 크롤링 CLI")

app.command()(crawl)
app.command()(auth)
app.command()(healthcheck)
app.command()(backfill)

if __name__ == "__main__":
    app()
