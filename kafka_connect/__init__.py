from collections import MutableMapping
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
import json

__all__ = ['KafkaConnect']

class Error(RuntimeError):
    retriable: bool = False
    code: int = 0

    def __str__(self):
        if not self.args:
            return self.__class__.__name__
        return '{0}: {1}'.format(self.__class__.__name__,
                                super().__str__())

class RebalanceError(Error):
    retriable = True
    code = 409

class NotFoundError(Error):
    code = 404


class TaskStatus:
    """ TaskStatus Type """

    __slots__ = 'id', 'state', 'worker_id'

    def __init__(self, id, state, worker_id):
        self.id = id
        self.state = state
        self.worker_id = worker_id


class Task:
    """ Task Type """

    __slots__ = '_api', 'id', 'connector', 'config'

    def __init__(self, api, id, connector, config):
        self._api = api
        self.id = id
        self.connector = connector
        self.config = config

    @property
    def status(self):
        response = self._api.get(
            '/connectors/{}/tasks/{}/status'.format(self.connector, self.id))
        id = response.get('id')
        state = response.get('state')
        worker_id = response.get('worker_id')
        return TaskStatus(id=id, state=state, worker_id=worker_id)

    def restart(self):
        self._api.post(
            'connectors/{}/tasks/{}/restart'.format(self.connector, self.id))


class Config(MutableMapping):
    """ Connector Config """

    __slots__ = '_api', '_config', '_connector'

    def __init__(self, api, connector):
        self._api = api
        self._connector = connector
        config = self._api.get("/connectors/{}/config".format(connector))
        self._config = config

    def __getitem__(self, name):
        return self._config[name]

    def __setitem__(self, name, value):
        self._config[name] = value
        self._commit()

    def __delitem__(self, name):
        del self._config[name]
        self._commit()

    def _commit(self):
        self._api.put(
            "/connectors/{}/config".format(self._connector), data=self._config)

    def __iter__(self):
        return iter(self._config)

    def __len__(self):
        return len(self._config)

    def keys(self):
        return self._config.keys()


class Connector:
    """ Connector Type """

    __slots__ = '_name', '_api'

    def __init__(self, api, name):
        self._name = name
        self._api = api

    # TODO: create setter to rename if possible
    @property
    def name(self):
        return self._name

    @property
    def config(self):
        _config = Config(self._api, self.name)
        return _config

    @config.setter
    def config(self, value):
        self._api.put(
            "/connectors/{}/config".format(self.name), data=value)

    @property
    def status(self):
        self._api.get('/connectors/{}/status'.format(self.name))

    def restart(self):
        self._api.post('/connectors/{}/restart'.format(self.name))

    def pause(self):
        self._api.put('/connectors/{}/pause'.format(self.name))

    def resume(self):
        self._api.put('/connectors/{}/resume'.format(self.name))

    @property
    def tasks(self):
        response = self._api.get('/connectors/{}/tasks'.format(self.name))
        _tasks = list()
        for task_dict in response:
            id_dict = task_dict.get('id')
            config = task_dict.get('config')
            id = id_dict.get('task')
            connector = id_dict.get('connector')
            task = Task(self._api, id, connector, config)
            _tasks.append(task)
        return _tasks


class Connectors(MutableMapping):
    """ Connectors """

    __slots__ = MutableMapping.__slots__ + ('_api',)

    def __init__(self, api):
        self._api = api

    def __getitem__(self, name):
        return Connector(self._api, name)

    def __setitem__(self, name, value):
        if isinstance(value, dict):
            config = value
        elif isinstance(value, Connector):
            config = value.config

        # TODO: test name in config
        if not 'name' in config:
            config['name'] = name

        if name in self.keys():
            self._api.put("/connectors/{}/config".format(name), data=config)
        else:
            self._api.post(
                "/connectors", data={'name': name, 'config': config})

    def __delitem__(self, name):
        self._api.delete("/connectors/{}".format(name))

    def __iter__(self):
        for key in self.keys():
            yield self.__getitem__(key)

    def __len__(self):
        return len(self.keys())

    def keys(self):
        return self._api.get('/connectors')


class API(object):
    """ Kafka Connect REST API Object """

    __slots__ = 'host', 'port', 'url', 'autocommit', 'version', 'commit', 'kafka_cluster_id'

    def __init__(self, host='localhost', port=8083, scheme='http', autocommit=True):
        self.host = host
        self.port = port
        self.url = "{}://{}:{}".format(scheme, host, port)
        self.autocommit = autocommit
        self.ping()

    def ping(self):
        info = self.get()
        self.version = info.get('version')
        self.commit = info.get('commit')
        self.kafka_cluster_id = info.get('kafka_cluster_id')

    def request(self, endpoint='/', method='GET'):
        if not endpoint.startswith('/'):
            endpoint = '/{}'.format(endpoint)
        request_url = "{}{}".format(self.url, endpoint)
        request = Request(request_url, method=method)
        request.add_header('Accept', 'application/json')
        request.add_header('Content-Type', 'application/json')
        return request

    def response(self, request):
        try:
            response = urlopen(request)
        except HTTPError as e:
            code = e.code
            if code == 404:
                raise NotFoundError()
            elif code == 409:
                raise RebalanceError()
            else:
                raise Error()
        except (URLError, ConnectionError, ConnectionResetError, ConnectionAbortedError):
            raise ConnectionError()
        except:
            raise Error()

        response_data = response.read()
        if response_data:
            response_dict = json.loads(response_data)
        else:
            response_dict = {}
        return response_dict

    def get(self, endpoint='/'):
        request = self.request(endpoint)
        return self.response(request)

    def post(self, endpoint='/', data=None):
        request = self.request(endpoint, method='POST')
        if data:
            request.data = json.dumps(data).encode('utf-8')
        return self.response(request)

    def put(self, endpoint='/', data=None):
        request = self.request(endpoint, method='PUT')
        if data:
            request.data = json.dumps(data).encode('utf-8')
        return self.response(request)

    def delete(self, endpoint='/'):
        request = self.request(endpoint, method='DELETE')
        return self.response(request)


class Plugin:
    """ Plugin Type """

    __slots__ = '_api', 'class_name', 'type_name', 'version'

    def __init__(self, api, class_name, type_name, version):
        self._api = api
        self.class_name = class_name
        self.type_name = type_name
        self.version = version

    def __repr__(self):
        return self.class_name

    def validate(self, config):
        return self._api.put('/connector-plugins/{}/config/validate/'.format(self.class_name.split('.')[-1]), data=config)


class KafkaConnect:
    """ Kafka Connect Object """

    __slots__ = 'api', 'connectors'

    def __init__(self, host='localhost', port=8083, scheme='http'):
        self.api = API(host=host, port=port, scheme=scheme)
        self.connectors = Connectors(self.api)

    @property
    def plugins(self):  # pylint: disable=unused-variable
        connector_plugins = self.api.get('/connector-plugins/')

        for plugin in connector_plugins:
            class_name = plugin.get('class')
            type_name = plugin.get('type')
            version = plugin.get('version')
            yield Plugin(self.api, class_name, type_name, version)
