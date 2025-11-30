import typer
from commands.crawl import crawl
from commands.download import download
from commands.auth import auth
from commands.healthcheck import healthcheck

app = typer.Typer(help="KRX 자동 크롤링 CLI")

app.command()(crawl)
app.command()(download)
app.command()(auth)
app.command()(healthcheck)

if __name__ == "__main__":
    app()
