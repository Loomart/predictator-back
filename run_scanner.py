from db import SessionLocal
from scanner import run_market_scanner


def main():
    db = SessionLocal()
    try:
        run_market_scanner(db)
    finally:
        db.close()


if __name__ == "__main__":
    main()