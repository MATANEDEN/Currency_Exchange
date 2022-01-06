import os
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from dotenv import load_dotenv

class Singleton(type):
    load_dotenv()
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class Database(metaclass=Singleton):
    connection =None
    def connect(self):
        DB = os.getenv("DB")
        PASSWORD = os.getenv("PASSWORD")
        USER = os.getenv("USER")
        PORT = os.getenv("PORT")

        try:
            self.engine = create_engine(f'postgresql://{USER}:{PASSWORD}@localhost:{PORT}/{DB}')
            self.connection = self.engine.connect()
            Session = sessionmaker(bind=self.engine)
            self.session = Session()

        except:
            print("error in postgresql")

    def close_connection(self):
        self.connection.close()