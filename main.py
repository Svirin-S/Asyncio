import asyncio
import aiohttp
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy import JSON, Integer, Column


PG_DSN = 'postgresql+asyncpg://app:12345678@127.0.0.1:5431/app'
engine = create_async_engine(PG_DSN)

Session = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()


class SwapiPeople(Base):

    __tablename__ = 'swapi_people'
    id = Column(Integer, primary_key=True)
    json = Column(JSON)


async def paste_to_db(people_list):
    async with Session() as session:
        orm_objects = [SwapiPeople(json=item) for item in people_list]
        session.add_all(orm_objects)
        await session.commit()


async def get_people(people_id, client_session):
    async with client_session.get(f'https://swapi.dev/api/people/{people_id}') as response:
        json_data = await response.json()
        data = {}
        for i in json_data:
            if i != 'created' and i != 'edited' and i != 'url':
                data[i] = json_data[i]
        return data


async def main():
    async with engine.begin() as con:
        await con.run_sync(Base.metadata.create_all)

    async with aiohttp.ClientSession() as client_session:
        coros = [get_people(i, client_session) for i in range(1, 6)]
        results = await asyncio.gather(*coros)
        print(results)

    async with Session() as session:
        orm_objects = [SwapiPeople(json=result) for result in results]
        session.add_all(orm_objects)
        await session.commit()

asyncio.run(main())