import sqlite3
import json

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


def extract():
    # REVIEW: нет описания возвращаемого значения в документации. В идеале стоит добавить типы аргументов и
    # возвращаемого значения с помощью модуля typing, см. https://docs.python.org/3/library/typing.html
    """
    extract data from sql-db
    :return:
    """
    connection = sqlite3.connect("db.sqlite")
    cursor = connection.cursor()

    # Наверняка это пилится в один sql - запрос, но мне как-то лениво)

    # Получаем все поля для индекса, кроме списка актеров и сценаристов, для них только id
    # REVIEW: посмотреть в сторону join-ов, чтобы не делать три запроса к базе
    cursor.execute("""
        select id, imdb_rating, genre, title, plot, director,
        -- comma-separated actor_id's
        (
            select GROUP_CONCAT(actor_id) from
            (
                select actor_id
                from movie_actors
                where movie_id = movies.id
            )
        ),

        max(writer, writers)
        from movies
    """)

    raw_data = cursor.fetchall()

    # REVIEW: закомментированный код стоит удалить.
    # cursor.execute('pragma table_info(movies)')
    # pprint(cursor.fetchall())

    # Нужны для соответсвия идентификатора и человекочитаемого названия
    actors = {row[0]: row[1] for row in cursor.execute('select * from actors where name != "N/A"')}
    writers = {row[0]: row[1] for row in cursor.execute('select * from writers where name != "N/A"')}

    return actors, writers, raw_data

# REVIEW: непонятен смысл нижних подчеркиваний у аргументов
def transform(__actors, __writers, __raw_data):
    # REVIEW: нет описания функции, аргументов и возвращаемого значения в документации + typing.
    """

    :param __actors:
    :param __writers:
    :param __raw_data:
    :return:
    """
    documents_list = []
    for movie_info in __raw_data:
        # Разыменование списка
        # REVIEW: если вдруг количество элементов в списке movie_info будет меньше или больше количества переменных в
        # левой части выражения, возникнет исключение. Можно обернуть в try-except, либо проверять длину списка перед
        # распаковкой.
        movie_id, imdb_rating, genre, title, description, director, raw_actors, raw_writers = movie_info

        if raw_writers[0] == '[':
            parsed = json.loads(raw_writers)
            new_writers = ','.join([writer_row['id'] for writer_row in parsed])
        else:
            new_writers = raw_writers

        writers_list = [(writer_id, __writers.get(writer_id)) for writer_id in new_writers.split(',')]
        actors_list = [(actor_id, __actors.get(int(actor_id))) for actor_id in raw_actors.split(',')]

        document = {
            "_index": "movies",
            "_id": movie_id,
            "id": movie_id,
            "imdb_rating": imdb_rating,
            "genre": genre.split(', '),
            "title": title,
            "description": description,
            "director": director,
            "actors": [
                {
                    "id": actor[0],
                    "name": actor[1]
                }
                # REVIEW: создается дополнительная структура set. Можно actors_list сразу сделать set-ом при создании.
                # REVIEW: иногда сложно понимать, что значат индексы [0], [1], это антипаттерн магические числа:
                # https://en.wikipedia.org/wiki/Magic_number_(programming). В данном случае лучше создавать структуры
                # типа словаря для однозначного понимания смысла того или иного значения по его ключу.
                for actor in set(actors_list) if actor[1]
            ],
            "writers": [
                {
                    "id": writer[0],
                    "name": writer[1]
                }
                # REVIEW: создается дополнительная структура set. Можно writers_list сразу сделать set-ом при создании.
                for writer in set(writers_list) if writer[1]
            ]
        }

        for key in document.keys():
            if document[key] == 'N/A':
                # REVIEW: закомментированный код стоит удалить.
                # print('hehe')
                document[key] = None

        document['actors_names'] = ", ".join([actor["name"] for actor in document['actors'] if actor]) or None
        document['writers_names'] = ", ".join([writer["name"] for writer in document['writers'] if writer]) or None

        # REVIEW: модули следует импортировать в начале файла. https://www.python.org/dev/peps/pep-0008/#imports
        import pprint
        # REVIEW: непонятно назначение этого вывода. Если оно нужно для показа проемежуточного результата, то стоит
        # это обозначить, например в комментарии, если оно нужно для отладки, то, наверное, стоит его удалить, поскольку
        # данных много, получется очень длинный вывод в консоль без каких-либо пояснений.
        pprint.pprint(document)

        documents_list.append(document)

    return documents_list


def load(acts):
    # REVIEW: пустая документация + typing.
    """

    :param acts:
    :return:
    """
    es = Elasticsearch([{'host': '192.168.1.252', 'port': 9200}])
    bulk(es, acts)

    return True


if __name__ == '__main__':
    # REVIEW: Функция load возращает значение, хотя при вызове этой функции оно не ожидается. Если подразумевается, что
    # вернувшееся True из функции load говорит об успешном выполнении скрипта, тогда стоит сделать проверку на True и
    # вывести в консоль сообщение об успешном выполнении, НО что будет, если возникнет исключение?
    # Лучше обернуть функицю load в try/except/else и в случае исключения (блок except) выодить текст ошибки, а в случае
    # успешного выполнения (блок else) выводить сообщение об успехе. Работа с исключениями:
    # https://docs.python.org/3/tutorial/errors.html

    # REVIEW: такую конструкцию load(transform(*extract())) довольно непросто сразу осознать. Возможно, стоит разделить
    # на два шага (вызова) с возвращаемыми значениями: 1 - extract, 2 - transform.
    load(transform(*extract()))
