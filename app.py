from flask import Flask, abort, request, jsonify
import elasticsearch as ES

# REVIEW: не найден модуль validate. Если это модуль из pip, необходимо добавить файл requirements.txt для загрузки
# необходимых модулей. Если это локальный файл - его нет в репозитории.
from validate import validate_args

app = Flask(__name__)


@app.route('/')
def index():
    return 'worked'


@app.route('/api/movies/')
def movie_list():
    # REVIEW: не найден модуль validate. Если это модуль из pip, необходимо добавить файл requirements.txt для загрузки
    # необходимых модулей. Если это локальный файл - его нет в репозитории.
    validate = validate_args(request.args)

    if not validate['success']:
        # REVIEW: при ошибках в запросе обычно должно возвращаться 400 Bad Request.
        # REVIEW: неясен код ошибки 422. Антипаттерн магические числа:
        # https://en.wikipedia.org/wiki/Magic_number_(programming). Лучше использовать библиотку http:
        # https://docs.python.org/3/library/http.html. В данном случае будет:
        # return abort(http.HTTPStatus.UNPROCESSABLE_ENTITY.value)
        return abort(422)

    # REVIEW: словарь defaults в дальнейшем может поменяться в зависимости от параметров запроса. Получается, что
    # значения в нем становятся уже не дефолтными. Стоит назвать его, например, init_params. Более того неясно его
    # предназначение, поскольку он просто хранит значения, которые потом подставляются в словарь params. Можно вынести
    # инициализацию дефолтными значениями в отдельный метод валидации (например, в validate_args) в рамках запроса и
    # подставлять значения, вернувшиеся из этого метода сразу в params.
    defaults = {
        'limit': 50,
        'page': 1,
        'sort': 'id',
        'sort_order': 'asc'
    }

    # Тут уже валидно все
    for param in request.args.keys():
        defaults[param] = request.args.get(param)

    # Уходит в тело запроса. Если запрос не пустой - мультисерч, если пустой - выдает все фильмы
    body = {
        "query": {
            "multi_match": {
                "query": defaults['search'],
                "fields": ["title"]
            }
        }
    } if defaults.get('search', False) else {}

    body['_source'] = dict()
    body['_source']['include'] = ['id', 'title', 'imdb_rating']

    params = {
        # REVIEW: закомментированный код стоит удалить.
        # '_source': ['id', 'title', 'imdb_rating'],
        'from': int(defaults['limit']) * (int(defaults['page']) - 1),
        'size': defaults['limit'],
        'sort': [
            {
                defaults["sort"]: defaults["sort_order"]
            }
        ]
    }

    # REVIEW: стоит один раз определить клиента ES на уровне модуля и не открывать/закрывать соединение на каждый
    # запрос.
    es_client = ES.Elasticsearch([{'host': '192.168.11.128', 'port': 9200}], )
    search_res = es_client.search(
        body=body,
        index='movies',
        params=params,
        filter_path=['hits.hits._source']
    )
    es_client.close()

    # REVIEW: лучше использовать конструкцию search_res.get('hits', {}).get('hits', []), если вдруг ключей hits не будет
    # в словаре.
    return jsonify([doc['_source'] for doc in search_res['hits']['hits']])


@app.route('/api/movies/<string:movie_id>')
def get_movie(movie_id):
    # REVIEW: стоит один раз определить клиента ES на уровне модуля и не открывать/закрывать соединение на каждый
    # запрос.
    es_client = ES.Elasticsearch([{'host': '192.168.11.128', 'port': 9200}], )

    # REVIEW: проверку на подключение к ES стоит провести при запуске сервиса в __name__ == "__main__" и, скорее всего,
    # не запускать сервис в случае отсутствия соединения. Если же необходимо, чтобы сервис работал, не смотря на
    # отсутствие соединения с ES, самый простой вариант - делать проверку es_client.ping() в начале каждого
    # обработчика запроса и, если соединения нет, отдавать ошибку, например 500 Internal server error.
    if not es_client.ping():
        print('oh(')

    search_result = es_client.get(index='movies', id=movie_id, ignore=404)

    es_client.close()

    if search_result['found']:
        return jsonify(search_result['_source'])

    # REVIEW: ответ 404 может также означать, что запрашиваемый ресурс (/api/movies/<string:movie_id>) не найден. В
    # данном случае необходимо помимо ответа 404 вернуть также тело ответа, в котором указать, например, что не найдено
    # фильма с таким id или, что количество найденных элементов = [].
    # REVIEW: неясен код ошибки 404. Лучше использовать библиотеку http: https://docs.python.org/3/library/http.html.
    return abort(404)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80)
