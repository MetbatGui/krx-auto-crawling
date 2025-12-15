import typer
from commands.crawl import crawl
app.command()(crawl)
app.command()(auth)
app.command()(healthcheck)

if __name__ == "__main__":
    app()
