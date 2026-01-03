"""Common utilities for scraping and data processing."""
import re
import random
import time
import logging
from typing import Optional, Set
from collections import defaultdict

logger = logging.getLogger(__name__)


def redact_secrets(message: str) -> str:
    """
    Redact secrets from log messages to prevent credential leakage.
    
    Args:
        message: Log message that may contain secrets
        
    Returns:
        Message with secrets redacted (replaced with ***)
    """
    if not message:
        return message
    
    # Redact connection strings: postgresql://user:password@host
    message = re.sub(
        r'://([^:]+):([^@]+)@',
        r'://\1:***@',
        message,
        flags=re.IGNORECASE
    )
    
    # Redact password environment variables: PASSWORD=value or PASSWORD: value
    message = re.sub(
        r'(password|secret|key|token)["\']?\s*[:=]\s*["\']?([^"\'\s]+)',
        r'\1="***"',
        message,
        flags=re.IGNORECASE
    )
    
    # Redact specific env var patterns
    message = re.sub(
        r'(POSTGRES_PASSWORD|WAREHOUSE_PASSWORD|AIRFLOW_PASSWORD|MINIO_SECRET_KEY|SUPABASE_DB_URL|PGADMIN_PASSWORD)["\']?\s*[:=]\s*["\']?([^"\'\s]+)',
        r'\1="***"',
        message,
        flags=re.IGNORECASE
    )
    
    return message

class BlockingDetectedError(Exception):
    """Raised when blocking is detected."""
    pass

def detect_blocking(html: str, url: str) -> tuple[bool, Optional[str]]:
    """
    Detect if we're being blocked (captcha, cloudflare, unusual traffic).
    Returns (is_blocked, reason).
    """
    html_lower = html.lower()
    
    if len(html) < 5000:
        if any(keyword in html_lower for keyword in ["captcha", "cloudflare", "challenge", "verify"]):
            return True, "Suspiciously short response with blocking keywords"
    
    blocking_patterns = [
        (r"cloudflare.*ray.*id", "Cloudflare protection page"),
        (r"checking.*browser.*before.*accessing", "Cloudflare challenge"),
        (r"please.*complete.*security.*check", "Security check required"),
        (r"unusual.*traffic.*detected", "Unusual traffic detected"),
        (r"access.*denied.*robot", "Access denied - robot"),
        (r"verify.*you.*are.*human", "Human verification required"),
        (r"captcha.*challenge", "CAPTCHA challenge page"),
    ]
    
    for pattern, reason in blocking_patterns:
        if re.search(pattern, html_lower, re.IGNORECASE):
            logger.warning(f"Blocking detected: {reason} on {url}")
            return True, reason
    
    title_match = re.search(r"<title[^>]*>([^<]+)</title>", html_lower)
    if title_match:
        title = title_match.group(1)
        if any(keyword in title for keyword in ["captcha", "challenge", "verify", "blocked", "access denied"]):
            return True, f"Blocking page title: {title[:50]}"
    
    if "cf-ray" in html_lower and len(html) < 20000:
        if "challenge" in html_lower or "checking" in html_lower:
            return True, "Cloudflare challenge page"
    
    return False, None

def random_sleep(min_ms: int, max_ms: int) -> None:
    """Sleep for a random duration between min_ms and max_ms milliseconds."""
    sleep_seconds = random.uniform(min_ms / 1000.0, max_ms / 1000.0)
    time.sleep(sleep_seconds)

def exponential_backoff_with_jitter(attempt: int, base: float = 1.0, max_wait: float = 300.0) -> float:
    """
    Calculate exponential backoff with jitter.
    Returns wait time in seconds.
    """
    wait = min(base * (2 ** attempt), max_wait)
    jitter = random.uniform(0, wait * 0.1)
    return wait + jitter

class StatsCollector:
    """Collect statistics about scraping operations."""
    
    def __init__(self) -> None:
        self.counts: dict[str, int] = defaultdict(int)
        self.status_codes: dict[int, int] = defaultdict(int)
        self.errors: list[str] = []
        self.reset()
    
    def reset(self) -> None:
        self.counts.clear()
        self.status_codes.clear()
        self.errors.clear()
        self.counts['discovered'] = 0
        self.counts['fetched'] = 0
        self.counts['parsed'] = 0
        self.counts['failed'] = 0
        self.counts['blocked'] = 0
        self.counts['duplicates'] = 0
    
    def record_discovered(self, count: int) -> None:
        """Record discovered listing count."""
        if count < 0:
            logger.warning(f"Negative discovered count: {count}")
            return
        self.counts['discovered'] += count
    
    def record_fetched(self, status_code: int) -> None:
        """Record a fetched listing with HTTP status code."""
        self.counts['fetched'] += 1
        self.status_codes[status_code] += 1
    
    def record_parsed(self) -> None:
        """Record a successfully parsed listing."""
        self.counts['parsed'] += 1
    
    def record_failed(self, error: str) -> None:
        """Record a failed operation with error message."""
        self.counts['failed'] += 1
        self.errors.append(error)
    
    def record_blocked(self) -> None:
        """Record a blocked request."""
        self.counts['blocked'] += 1
    
    def record_duplicate(self) -> None:
        """Record a duplicate listing."""
        self.counts['duplicates'] += 1
    
    def get_summary(self) -> dict[str, int | dict | list[str]]:
        """Get summary statistics."""
        return {
            'counts': dict(self.counts),
            'status_codes': dict(self.status_codes),
            'error_count': len(self.errors),
            'recent_errors': self.errors[-10:] if self.errors else []
        }
    
    def log_summary(self) -> None:
        """Log summary to logger."""
        summary = self.get_summary()
        logger.info("=== Scraping Statistics ===")
        logger.info(f"Discovered: {summary['counts'].get('discovered', 0)}")
        logger.info(f"Fetched: {summary['counts'].get('fetched', 0)}")
        logger.info(f"Parsed: {summary['counts'].get('parsed', 0)}")
        logger.info(f"Failed: {summary['counts'].get('failed', 0)}")
        logger.info(f"Blocked: {summary['counts'].get('blocked', 0)}")
        logger.info(f"Duplicates: {summary['counts'].get('duplicates', 0)}")
        logger.info(f"Status codes: {summary['status_codes']}")
        if summary['error_count'] > 0:
            logger.warning(f"Errors encountered: {summary['error_count']}")
            for err in summary['recent_errors']:
                logger.warning(f"  - {err}")

