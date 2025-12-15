import typer
import warnings

# Suppress benign openpyxl warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

from commands.crawl import crawl
from commands.auth import auth
from commands.healthcheck import healthcheck

app = typer.Typer(help="KRX 자동 크롤링 CLI")

app.command()(crawl)
app.command()(auth)
app.command()(healthcheck)

if __name__ == "__main__":
    app()
