import logging
import os
from logging.handlers import RotatingFileHandler

def setup_logger(name: str = "krx_netbuy") -> logging.Logger:
    """애플리케이션 전역 로거 설정을 초기화하고 반환합니다."""
    logger = logging.getLogger(name)
    
    # 이미 핸들러가 설정되어 있다면 기존 로거 반환
    if logger.handlers:
        return logger

    # 로그 레벨 설정 (기본값: INFO)
    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    logger.setLevel(log_level)

    # 로그 포맷 정의 (이모지 제외)
    log_format = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # 1. 콘솔 핸들러 설정
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)

    # 2. 파일 핸들러 설정 (output/logs/app.log)
    try:
        log_dir = os.path.join("output", "logs")
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, "app.log")
        
        # 최대 10MB 크기, 백업 파일 5개 관리
        file_handler = RotatingFileHandler(
            log_file, 
            maxBytes=10*1024*1024, 
            backupCount=5, 
            encoding="utf-8"
        )
        file_handler.setFormatter(log_format)
        logger.addHandler(file_handler)
    except Exception as e:
        # 파일 시스템 권한 등의 이유로 파일 로그 실패 시 콘솔 경고 출력
        console_handler.setLevel(logging.WARNING)
        logger.warning(f"Failed to setup file logging: {e}")

    return logger

# 전역 기본 로거 인스턴스 제공
logger = setup_logger()
