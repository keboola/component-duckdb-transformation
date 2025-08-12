"""System resource detection and DuckDB optimization module."""

import os
import logging
from typing import Optional

# Constants for optimization
# Reserve a fixed amount of memory for Python runtime and overhead
PYTHON_RESERVED_MEMORY_MB = 256


def detect_cpu_count() -> Optional[int]:
    """Detect CPU count from cgroup."""
    # Try cgroup v1
    cpu_quota_path = "/sys/fs/cgroup/cpu/cpu.cfs_quota_us"
    cpu_period_path = "/sys/fs/cgroup/cpu/cpu.cfs_period_us"

    if os.path.exists(cpu_quota_path) and os.path.exists(cpu_period_path):
        try:
            with open(cpu_quota_path, "r") as f:
                quota = int(f.read().strip())
            with open(cpu_period_path, "r") as f:
                period = int(f.read().strip())

            if quota > 0:
                cpu_count = max(1, quota // period)
                logging.debug(f"cgroup v1 CPU detected: quota={quota}, period={period}, count={cpu_count}")
                return cpu_count
        except (ValueError, OSError, IOError) as e:
            logging.debug(f"cgroup v1 CPU detection failed: {e}")
        except Exception as e:
            logging.debug(f"Unexpected error in cgroup v1 CPU detection: {e}")

    # Try cgroup v2
    cpu_max_path = "/sys/fs/cgroup/cpu.max"
    if os.path.exists(cpu_max_path):
        try:
            with open(cpu_max_path, "r") as f:
                content = f.read().strip()
                if content != "max":
                    quota, period = map(int, content.split())
                    cpu_count = max(1, quota // period)
                    logging.debug(f"cgroup v2 CPU detected: quota={quota}, period={period}, count={cpu_count}")
                    return cpu_count
        except (ValueError, OSError, IOError) as e:
            logging.debug(f"cgroup v2 CPU detection failed: {e}")
        except Exception as e:
            logging.debug(f"Unexpected error in cgroup v2 CPU detection: {e}")

    return None


def detect_memory_mb() -> Optional[int]:
    """Detect memory limit from cgroup."""
    # Try cgroup v1
    memory_limit_path = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
    if os.path.exists(memory_limit_path):
        try:
            with open(memory_limit_path, "r") as f:
                memory_bytes = int(f.read().strip())
                if memory_bytes > 0:
                    memory_mb = memory_bytes // (1024 * 1024)
                    logging.debug(f"cgroup v1 memory detected: {memory_bytes} bytes = {memory_mb}MB")
                    return memory_mb
        except (ValueError, OSError, IOError) as e:
            logging.debug(f"cgroup v1 memory detection failed: {e}")
        except Exception as e:
            logging.debug(f"Unexpected error in cgroup v1 memory detection: {e}")

    # Try cgroup v2
    memory_max_path = "/sys/fs/cgroup/memory.max"
    if os.path.exists(memory_max_path):
        try:
            with open(memory_max_path, "r") as f:
                content = f.read().strip()
                if content != "max":
                    memory_bytes = int(content)
                    memory_mb = memory_bytes // (1024 * 1024)
                    logging.debug(f"cgroup v2 memory detected: {memory_bytes} bytes = {memory_mb}MB")
                    return memory_mb
        except (ValueError, OSError, IOError) as e:
            logging.debug(f"cgroup v2 memory detection failed: {e}")
        except Exception as e:
            logging.debug(f"Unexpected error in cgroup v2 memory detection: {e}")

    return None


def get_optimal_memory_mb() -> Optional[int]:
    """Return detected memory minus a fixed Python reserve (in MB)."""
    try:
        detected_memory = detect_memory_mb()
        if detected_memory is None:
            logging.warning("Memory detection returned None")
            return None
        usable_memory = max(1, int(detected_memory) - PYTHON_RESERVED_MEMORY_MB)
        logging.info(
            f"Detected memory: {detected_memory}MB, reserving {PYTHON_RESERVED_MEMORY_MB}MB for Python"
            f", using: {usable_memory}MB"
        )
        return usable_memory
    except (ValueError, TypeError) as e:
        logging.warning(f"Memory calculation error: {e}")
        return None
    except Exception as e:
        logging.warning(f"Unexpected error in memory optimization: {e}")
        return None
