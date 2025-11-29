from typer.testing import CliRunner
from src.cli import app

runner = CliRunner()

def test_cli_help():
    """CLI 도움말이 정상적으로 출력되는지 확인"""
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "KRX Auto Crawling CLI" in result.stdout
    assert "crawl" in result.stdout
    assert "download" in result.stdout

def test_cli_crawl_help():
    """crawl 명령어 도움말 확인"""
    result = runner.invoke(app, ["crawl", "--help"])
    assert result.exit_code == 0
    assert "Execute the daily crawling routine" in result.stdout

def test_cli_crawl_invalid_date_format():
    """잘못된 날짜 형식을 입력했을 때 에러가 발생하는지 확인"""
    result = runner.invoke(app, ["crawl", "2025-01-01"])  # Hyphen included
    assert result.exit_code == 1
    assert "Invalid date format" in result.output

def test_cli_download_help():
    """download 명령어 도움말 확인"""
    result = runner.invoke(app, ["download", "--help"])
    assert result.exit_code == 0
    assert "Download files from Google Drive" in result.stdout
