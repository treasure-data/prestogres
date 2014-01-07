import os
import httplib
import time

try: import simplejson as json
except ImportError: import json

VERSION = "0.1.0"

class ClientSession(object):
    def __init__(self, server, user, source=None, catalog=None, schema=None, debug=False):
        self.server = server
        self.user = user
        self.source = source
        self.catalog = catalog
        self.schema = schema
        self.debug = debug

class StatementStats(object):
    def __init__(self, state=None, scheduled=None, nodes=None, total_splits=None, queued_splits=None, running_splits=None, completed_splits=None, user_time_millis=None, cpu_time_millis=None, wall_time_millis=None, processed_rows=None, processed_bytes=None):
        self.state = state
        self.scheduled = scheduled
        self.nodes = nodes
        self.total_splits = total_splits
        self.queued_splits = queued_splits
        self.running_splits = running_splits
        self.completed_splits = completed_splits
        self.user_time_millis = user_time_millis
        self.cpu_time_millis = cpu_time_millis
        self.wall_time_millis = wall_time_millis
        self.processed_rows = processed_rows
        self.processed_bytes = processed_bytes
        #self.root_stage = root_stage

    @classmethod
    def decode_dict(cls, dic):
        return StatementStats(
                state = dic.get("state"),
                scheduled = dic.get("scheduled"),
                nodes = dic.get("nodes"),
                total_splits = dic.get("totalSplits"),
                queued_splits = dic.get("queuedSplits"),
                running_splits = dic.get("runningSplits"),
                completed_splits = dic.get("completedSplits"),
                user_time_millis = dic.get("userTimeMillis"),
                cpu_time_millis = dic.get("cpuTimeMillis"),
                wall_time_millis = dic.get("wallTimeMillis"),
                processed_rows = dic.get("processedRows"),
                processed_bytes = dic.get("processedBytes"),
                #root_stage = StageStats.decode_dict(dic["rootStage")),
                )

class Column(object):
    def __init__(self, name, type):
        self.name = name
        self.type = type

    @classmethod
    def decode_dict(cls, dic):
        return Column(
                name = dic.get("name"),
                type = dic.get("type"),
                )

class QueryResults(object):
    def __init__(self, id, info_uri=None, partial_cache_uri=None, next_uri=None, columns=None, data=None, stats=None, error=None):
        self.id = id
        self.info_uri = info_uri
        self.partial_cache_uri = partial_cache_uri
        self.next_uri = next_uri
        self.columns = columns
        self.data = data
        self.stats = stats
        self.error = error

    @classmethod
    def decode_dict(cls, dic):
        return QueryResults(
                id = dic.get("id"),
                info_uri = dic.get("infoUri"),
                partial_cache_uri = dic.get("partialCancelUri"),
                next_uri = dic.get("nextUri"),
                columns = map(Column.decode_dict, dic["columns"]) if "columns" in dic else None,
                data = dic.get("data"),
                stats = StatementStats.decode_dict(dic["stats"]),
                error = dic.get("error"),  # TODO
                )

class PrestoHeaders(object):
    PRESTO_USER = "X-Presto-User"
    PRESTO_SOURCE = "X-Presto-Source"
    PRESTO_CATALOG = "X-Presto-Catalog"
    PRESTO_SCHEMA = "X-Presto-Schema"

    PRESTO_CURRENT_STATE = "X-Presto-Current-State"
    PRESTO_MAX_WAIT = "X-Presto-Max-Wait"
    PRESTO_MAX_SIZE = "X-Presto-Max-Size"
    PRESTO_PAGE_SEQUENCE_ID = "X-Presto-Page-Sequence-Id"

class StatementClient(object):
    HEADERS = {
            "User-Agent": "presto-python/%s" % VERSION
            }

    def __init__(self, http_client, session, query):
        self.http_client = http_client
        self.session = session
        self.query = query

        self.closed = False
        self.exception = None
        self.results = None
        self._post_query_request()

    def _post_query_request(self):
        headers = StatementClient.HEADERS.copy()

        if self.session.user is not None:
            headers[PrestoHeaders.PRESTO_USER] = self.session.user
        if self.session.source is not None:
            headers[PrestoHeaders.PRESTO_SOURCE] = self.session.source
        if self.session.catalog is not None:
            headers[PrestoHeaders.PRESTO_CATALOG] = self.session.catalog
        if self.session.schema is not None:
            headers[PrestoHeaders.PRESTO_SCHEMA] = self.session.schema

        self.http_client.request("POST", "/v1/statement", self.query, headers)
        response = self.http_client.getresponse()
        body = response.read()

        if response.status != 200:
            raise Exception("Failed to start query: %s" % body)

        dic = json.loads(body)
        self.results = QueryResults.decode_dict(dic)

    @property
    def is_query_failed(self):
        return self.results.error is not None

    @property
    def is_query_succeeded(self):
        return self.results.error is None and self.exception is None and not self.closed

    @property
    def has_next(self):
        return self.results.next_uri is not None

    def advance(self):
        if self.closed or not self.has_next:
            return False

        uri = self.results.next_uri
        start = time.time()
        attempts = 0

        while True:
            try:
                self.http_client.request("GET", uri)
            except Exception as e:
                self.exception = e
                raise

            response = self.http_client.getresponse()
            body = response.read()

            if response.status == 200 and body:
                self.results = QueryResults.decode_dict(json.loads(body))
                return True

            if response.status != 503:  # retry on 503 Service Unavailable
                # deterministic error
                self.exception = Exception("Error fetching next at %s returned %s: %s" % (uri, response.status, body))  # TODO error class
                raise self.exception

            if (time.time() - start) > 2*60*60 or self.closed:
                break

        self.exception = Exception("Error fetching next")  # TODO error class
        raise self.exception

    def close(self):
        if self.closed:
            return

        if self.results.next_uri is not None:
            self.http_client.request("DELETE", self.results.next_uri)

        self.closed = True

class Query(object):
    @classmethod
    def start(cls, session, query):
        http_client = httplib.HTTPConnection(session.server)
        return Query(StatementClient(http_client, session, query))

    def __init__(self, client):
        self.client = client

    def _wait_for_data(self):
        while self.client.has_next and self.client.results.data is None:
            self.client.advance()

    def columns(self):
        self._wait_for_data()

        if not self.client.is_query_succeeded:
            self._raise_error()

        return self.client.results.columns

    def results(self):
        client = self.client

        if not client.is_query_succeeded:
            self._raise_error()

        if self.columns() is None:
            raise Exception("Query %s has no columns" % client.results.id)

        while True:
            for row in client.results.data:
                yield row

            if not self.client.has_next:
                break

            client.advance()
            if client.results.data is None:
                break

    def _raise_error(self):
        if self.client.closed:
            raise Exception("Query aborted by user")
        elif self.client.exception is not None:
            raise Exception("Query is gone: %s" % self.client.exception)
        elif self.client.is_query_failed:
            results = self.client.results
            raise Exception("Query %s failed: %s" % (results.id, results.error))

