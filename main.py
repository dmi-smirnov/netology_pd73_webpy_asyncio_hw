import asyncio

import aiohttp

import db


GET_PEOPLE_URL = 'https://swapi.dev/api/people/'

async def get_people_list(URL: str):
    url = URL
    while url:
        async with aiohttp.ClientSession() as aiohttp_session:
            resp = await aiohttp_session.get(url)
            json_data =  await resp.json()
        if isinstance(json_data, dict):
            url = json_data.get('next')
            results = json_data.get('results')
            if results and isinstance(results, list):
                yield results

def async_cached(old_func):
    cache = dict()
    func_calls_count = 0
    cache_calls_count = 0

    async def new_func(*args, **kwargs):
        nonlocal func_calls_count
        func_calls_count += 1
        key = f'{args}_{kwargs}'
        if key in cache:
            cache_value = cache[key]
            if isinstance(cache_value, asyncio.Task):
                cache_value_task = cache_value
                print(f'Ожидание кэша для {key}')
                await cache_value_task
            nonlocal cache_calls_count
            cache_calls_count += 1
            print(f'Использован кэш для {key}.'
                  f' Кэш использован в {cache_calls_count} вызовах'
                  f' из {func_calls_count}.')
            return cache[key]
        old_func_task = asyncio.create_task(old_func(*args, **kwargs))
        cache[key] = old_func_task
        print(f'Первый запрос для {key}')
        cache[key] = await old_func_task
        print(f'Создан кэш для {key}')
        return cache[key]
    return new_func

@async_cached
async def aiohttp_get_json(url: str):
    async with aiohttp.ClientSession() as aiohttp_session:
        resp = await aiohttp_session.get(url)
        return await resp.json()

async def get_concat_values(urls: list[str], value_name: str):
    if not urls:
        return
    result = ''
    for url in urls:
        json_data =  await aiohttp_get_json(url)
        if not isinstance(json_data, dict):
            continue
        if not value_name in json_data:
            continue
        result += f', {json_data[value_name]}'
    return result

def get_id_from_url(url: str) -> int:
    cur = -1
    id_str = ''
    if not url[cur].isnumeric():
        cur -= 1
    while url[cur].isnumeric():
        id_str = url[cur] + id_str
        cur -= 1
    return int(id_str)
    
async def insert_person(db_init_task: asyncio.Task, person: dict) -> None:
    get_person_films_task =\
        asyncio.create_task(get_concat_values(person['films'], 'title'))
    get_person_species_task =\
        asyncio.create_task(get_concat_values(person['species'], 'name'))
    get_person_starships_task =\
        asyncio.create_task(get_concat_values(person['starships'], 'name'))
    get_person_vehicles_task =\
        asyncio.create_task(get_concat_values(person['vehicles'], 'name'))
    
    person_db_data = dict()

    ready_items_keys = [
        'birth_year',
        'eye_color',
        'gender',
        'hair_color',
        'homeworld',
        'name',
        'skin_color',
        'height',
        'mass'
    ]
    for key in ready_items_keys:
        if key in person:
            person_db_data[key] = person[key]
    
    person_db_data['id'] = get_id_from_url(person['url'])

    person_db_data['films'] = await get_person_films_task
    person_db_data['species'] = await get_person_species_task
    person_db_data['starships'] = await get_person_starships_task
    person_db_data['vehicles'] = await get_person_vehicles_task

    await db_init_task

    async with db.Session() as db_session:
        db_session.add(db.StarWarsPerson(**person_db_data))
        await db_session.commit()

    global db_count
    db_count += 1

    print(f'Добавление в БД №{db_count}:'
          f' id_{person_db_data["id"]} {person_db_data["name"]}')

async def insert_people_list(db_init_task: asyncio.Task,
                             people_list: list) -> None:
    for person in people_list:
        asyncio.create_task(insert_person(db_init_task, person))

async def main():
    db_init_task = asyncio.create_task(db.drop_all_and_create_all())

    async for people_list in get_people_list(GET_PEOPLE_URL):
        global api_count
        
        print(f'Получены по API ({api_count} + {len(people_list)}):',
              end='\n  ')
        print('\n  '.join([p['name'] for p in people_list]))
        
        api_count += len(people_list)

        asyncio.create_task(insert_people_list(db_init_task, people_list))
    
    while len(asyncio.all_tasks()) > 1:
        active_tasks = asyncio.all_tasks() - {asyncio.current_task()}
        await asyncio.gather(*active_tasks)


if __name__ == '__main__':
    api_count = 0
    db_count = 0

    asyncio.run(main())

    print(f'\n\nВыполнение завершено.')
    print(f'Получено из API: {api_count}')
    print(f'Добавлено в БД: {db_count}')
