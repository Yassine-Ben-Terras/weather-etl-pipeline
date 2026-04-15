"""
logger.py — Centralized structured logger using Loguru.
Writes to both console and rotating log file.
"""
import sys
from loguru import logger
from pathlib import Path
from src.config import LOG_LEVEL

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Remove default handler
logger.remove()

# Console handler — colored, human-readable
logger.add(
    sys.stdout,
    level=LOG_LEVEL,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> — <level>{message}</level>",
    colorize=True,
)

# File handler — structured JSON logs, rotating daily, kept 7 days
logger.add(
    LOG_DIR / "pipeline_{time:YYYY-MM-DD}.log",
    level="DEBUG",
    rotation="00:00",
    retention="7 days",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} — {message}",
    encoding="utf-8",
)
