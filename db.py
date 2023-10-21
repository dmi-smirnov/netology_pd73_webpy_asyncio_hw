import os

import sqlalchemy as sql
import sqlalchemy.orm as sql_orm
import sqlalchemy.ext.asyncio as sql_asyncio


DBMS = 'postgresql+asyncpg'
DB_HOST_ADDR = os.getenv('POSTGRESQL_HOST_ADDR', '127.0.0.1')
DB_HOST_PORT = os.getenv('POSTGRESQL_HOST_PORT', '5432')
DB_USER = os.getenv('POSTGRESQL_USER', 'test')
DB_PWD = os.getenv('POSTGRESQL_PWD', 'test')
DB_NAME = os.getenv('POSTGRESQL_DB', 'test')

DSN = \
    f'{DBMS}://{DB_USER}:{DB_PWD}@{DB_HOST_ADDR}:{DB_HOST_PORT}/{DB_NAME}'

engine = sql_asyncio.create_async_engine(DSN)

BaseClass = sql_orm.declarative_base()

Session = sql_orm.sessionmaker(class_=sql_asyncio.AsyncSession,
                               expire_on_commit=False,
                               bind=engine)

class StarWarsPerson(BaseClass):
    __tablename__ = 'starwars_person'

    id = sql.Column(sql.Integer, primary_key=True)
    birth_year = sql.Column(sql.String)
    eye_color = sql.Column(sql.String)
    films = sql.Column(sql.String)
    gender = sql.Column(sql.String)
    hair_color = sql.Column(sql.String)
    height = sql.Column(sql.String)
    homeworld = sql.Column(sql.String)
    mass = sql.Column(sql.String)
    name = sql.Column(sql.String)
    skin_color = sql.Column(sql.String)
    species = sql.Column(sql.String)
    starships = sql.Column(sql.String)
    vehicles = sql.Column(sql.String)


async def drop_all_and_create_all():
    async with engine.begin() as conn:
        await conn.run_sync(BaseClass.metadata.drop_all)
        await conn.run_sync(BaseClass.metadata.create_all)

async def dispose():
    await engine.dispose()