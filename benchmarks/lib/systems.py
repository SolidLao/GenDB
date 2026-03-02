"""System lifecycle: install, start, stop, available for ClickHouse, Umbra, MonetDB."""

import os
import shutil
import subprocess
import time

import psycopg2


# ---------------------------------------------------------------------------
# ClickHouse
# ---------------------------------------------------------------------------

def clickhouse_install(benchmark_root):
    """Download ClickHouse single binary."""
    ch_dir = benchmark_root / "clickhouse"
    ch_dir.mkdir(exist_ok=True)
    binary = ch_dir / "clickhouse"
    if binary.exists() and os.access(str(binary), os.X_OK):
        print("  ClickHouse binary already installed.")
        return True
    print("  Downloading ClickHouse...")
    try:
        subprocess.run(
            ["bash", "-c", f"cd {ch_dir} && curl -fsSL https://clickhouse.com/ | sh"],
            check=True, capture_output=True, timeout=120,
        )
        if binary.exists():
            print("  ClickHouse installed successfully.")
            return True
        for f in ch_dir.iterdir():
            if f.name.startswith("clickhouse") and os.access(str(f), os.X_OK):
                if f.name != "clickhouse":
                    f.rename(binary)
                print("  ClickHouse installed successfully.")
                return True
        print("  ClickHouse installation failed: binary not found after download.")
        return False
    except Exception as e:
        print(f"  ClickHouse installation failed: {e}")
        return False


def clickhouse_available(benchmark_root) -> bool:
    binary = benchmark_root / "clickhouse" / "clickhouse"
    return binary.exists() and os.access(str(binary), os.X_OK)


def clickhouse_server_start(benchmark_root, config):
    """Start a ClickHouse server process."""
    ch_dir = benchmark_root / "clickhouse"
    binary = ch_dir / "clickhouse"
    data_path = ch_dir / f"data_{config.db_name}"
    data_path.mkdir(parents=True, exist_ok=True)
    log_path = ch_dir / f"server_{config.db_name}.log"

    proc = subprocess.Popen(
        [str(binary), "server",
         "--daemon",
         "--",
         f"--path={data_path}",
         "--tcp_port=9000",
         "--http_port=8123",
         "--mysql_port=0",
         "--interserver_http_port=0",
         f"--logger.log={log_path}",
         f"--logger.errorlog={log_path}"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    for attempt in range(30):
        time.sleep(1)
        try:
            result = subprocess.run(
                [str(binary), "client", "--port", "9000", "-q", "SELECT 1"],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0:
                print(f"  ClickHouse server started.")
                return proc
        except Exception:
            pass
    print("  Warning: ClickHouse server may not be ready.")
    return proc


def clickhouse_server_stop(proc, benchmark_root):
    """Stop ClickHouse server."""
    ch_dir = benchmark_root / "clickhouse"
    binary = ch_dir / "clickhouse"
    try:
        subprocess.run(
            [str(binary), "client", "--port", "9000", "-q", "SYSTEM SHUTDOWN"],
            capture_output=True, timeout=10,
        )
    except Exception:
        pass
    if proc:
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
    print("  ClickHouse server stopped.")


# ---------------------------------------------------------------------------
# Umbra (Docker-based)
# ---------------------------------------------------------------------------

def umbra_install():
    """Pull the Umbra Docker image."""
    if not shutil.which("docker"):
        print("  Docker not available, cannot install Umbra.")
        return False
    print("  Pulling Umbra Docker image...")
    try:
        subprocess.run(
            ["docker", "pull", "umbradb/umbra:latest"],
            check=True, capture_output=True, timeout=600,
        )
        print("  Umbra image pulled successfully.")
        return True
    except Exception as e:
        print(f"  Umbra pull failed: {e}")
        return False


def umbra_available() -> bool:
    if not shutil.which("docker"):
        return False
    try:
        result = subprocess.run(
            ["docker", "images", "-q", "umbradb/umbra"],
            capture_output=True, text=True, timeout=10,
        )
        return result.returncode == 0 and result.stdout.strip() != ""
    except Exception:
        return False


def umbra_start(config) -> str:
    """Start Umbra Docker container. Reuses existing container if running."""
    container_name = f"umbra_{config.db_name}"
    port = config.umbra_port

    result = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Status}}", container_name],
        capture_output=True, text=True, timeout=10,
    )
    if result.returncode == 0:
        status = result.stdout.strip()
        if status == "running":
            try:
                conn = psycopg2.connect(
                    host="127.0.0.1", port=port, user="postgres",
                    password="postgres", dbname="postgres", connect_timeout=3,
                )
                conn.close()
                print(f"  Umbra container '{container_name}' already running (port {port}).")
                return container_name
            except Exception:
                pass
        if status in ("exited", "created"):
            print(f"  Restarting existing Umbra container '{container_name}'...")
            subprocess.run(
                ["docker", "start", container_name],
                capture_output=True, timeout=30,
            )
            for attempt in range(60):
                time.sleep(1)
                try:
                    conn = psycopg2.connect(
                        host="127.0.0.1", port=port, user="postgres",
                        password="postgres", dbname="postgres", connect_timeout=3,
                    )
                    conn.close()
                    print(f"  Umbra container restarted and ready (port {port}).")
                    return container_name
                except Exception:
                    pass
            print("  Warning: Umbra may not be ready after restart.")
            return container_name

    # No existing container — remove any broken remnant and create fresh
    subprocess.run(
        ["docker", "rm", "-f", container_name],
        capture_output=True, timeout=10,
    )
    print(f"  Starting Umbra container '{container_name}'...")
    # Mount benchmark_root for fast COPY FROM inside container
    mount_dir = config.data_dir if config.data_dir else config.benchmark_root
    subprocess.run(
        ["docker", "run", "-d",
         "--name", container_name,
         "--privileged",
         "--ulimit", "memlock=-1:-1",
         "--shm-size=4g",
         "-p", f"{port}:5432",
         "-v", f"{mount_dir}:/data",
         "umbradb/umbra:latest"],
        check=True, capture_output=True, timeout=30,
    )
    for attempt in range(60):
        time.sleep(1)
        try:
            conn = psycopg2.connect(
                host="127.0.0.1", port=port, user="postgres",
                password="postgres", dbname="postgres", connect_timeout=3,
            )
            conn.close()
            print(f"  Umbra container started and ready (port {port}).")
            return container_name
        except Exception:
            pass
    print("  Warning: Umbra may not be ready.")
    return container_name


def umbra_stop(container_name: str):
    """Stop Umbra container (keep it so data persists for next run)."""
    subprocess.run(
        ["docker", "stop", container_name],
        capture_output=True, timeout=30,
    )
    print(f"  Umbra container '{container_name}' stopped (data preserved).")


# ---------------------------------------------------------------------------
# MonetDB
# ---------------------------------------------------------------------------

def monetdb_available() -> bool:
    return shutil.which("monetdbd") is not None or shutil.which("mserver5") is not None


def monetdb_server_start(config):
    """Start MonetDB server."""
    farm_dir = config.benchmark_root / "monetdb" / f"farm_{config.db_name}"
    farm_dir.mkdir(parents=True, exist_ok=True)
    dbname = config.db_name

    # Check if already running by trying to connect
    try:
        import pymonetdb
        conn = pymonetdb.connect(database=dbname, hostname="localhost",
                                 port=50000, username="monetdb", password="monetdb")
        conn.close()
        print(f"  MonetDB already running with database '{dbname}'.")
        return farm_dir
    except Exception:
        pass

    # Create farm if needed
    try:
        subprocess.run(
            ["monetdbd", "create", str(farm_dir)],
            capture_output=True, text=True, timeout=10,
        )
    except Exception:
        pass

    # Set port
    subprocess.run(
        ["monetdbd", "set", "port=50000", str(farm_dir)],
        capture_output=True, timeout=10,
    )

    # Start farm
    os.system(f"monetdbd start '{farm_dir}'")

    # Wait for daemon to be ready
    for attempt in range(30):
        time.sleep(1)
        try:
            result = subprocess.run(
                ["monetdb", "-p", "50000", "status"],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0:
                break
        except Exception:
            pass

    # Create database if needed
    try:
        subprocess.run(
            ["monetdb", "-p", "50000", "create", dbname],
            capture_output=True, text=True, timeout=10,
        )
    except Exception:
        pass
    subprocess.run(
        ["monetdb", "-p", "50000", "release", dbname],
        capture_output=True, text=True, timeout=10,
    )
    time.sleep(1)
    print(f"  MonetDB server started with database '{dbname}'.")
    return farm_dir


def monetdb_server_stop(farm_dir):
    """Stop MonetDB server."""
    if farm_dir:
        os.system(f"monetdbd stop '{farm_dir}'")
        time.sleep(2)
        print("  MonetDB server stopped.")


def mclient(dbname: str, sql: str, timeout: int = 600):
    """Run a SQL statement via mclient (fast, server-side execution)."""
    return subprocess.run(
        ["mclient", "-p", "50000", "-d", dbname, "-s", sql],
        check=True, capture_output=True, text=True, timeout=timeout,
    )
