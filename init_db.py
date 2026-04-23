from db import engine
from models import Base


def init_db():
    Base.metadata.create_all(bind=engine)
    print("Tablas creadas correctamente.")


if __name__ == "__main__":
    init_db()