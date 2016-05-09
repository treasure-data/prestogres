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
                state=dic.get("state"),
                scheduled=dic.get("scheduled"),
                nodes=dic.get("nodes"),
                total_splits=dic.get("totalSplits"),
                queued_splits=dic.get("queuedSplits"),
                running_splits=dic.get("runningSplits"),
                completed_splits=dic.get("completedSplits"),
                user_time_millis=dic.get("userTimeMillis"),
                cpu_time_millis=dic.get("cpuTimeMillis"),
                wall_time_millis=dic.get("wallTimeMillis"),
                processed_rows=dic.get("processedRows"),
                processed_bytes=dic.get("processedBytes"),
                #root_stage=StageStats.decode_dict(dic["rootStage")),
                )

class Column(object):
    def __init__(self, name, type):
        self.name = name
        self.type = type

    @classmethod
    def decode_dict(cls, dic):
        return Column(
                name=dic.get("name"),
                type=dic.get("type"),
                )

class ErrorLocation(object):
    def __init__(self, line_number, column_number):
        self.line_number = line_number
        self.column_number = column_number

    @classmethod
    def decode_dict(cls, dic):
        return ErrorLocation(
                line_number=dic.get("lineNumber"),
                column_number=dic.get("columnNumber"),
                )

class FailureInfo(object):
    def __init__(self, type=None, message=None, cause=None, suppressed=None, stack=None, error_location=None):
        self.type = type
        self.message = message
        self.cause = cause
        self.suppressed = suppressed
        self.stack = stack
        self.error_location = error_location

    @classmethod
    def decode_dict(cls, dic):
        return FailureInfo(
                type=dic.get("type"),
                message=dic.get("message"),
                cause=dic.get("cause"),
                suppressed=map(FailureInfo.decode_dict, dic["suppressed"]) if "suppressed" in dic else None,
                stack=dic.get("stack"),
                error_location=ErrorLocation.decode_dict(dic["errorLocation"]) if "errorLocation" in dic else None,
                )

class QueryError(object):
    def __init__(self, message=None, sql_state=None, error_code=None, error_location=None, failure_info=None):
        self.message = message
        self.sql_state = sql_state
        self.error_code = error_code
        self.error_location = error_location
        self.failure_info = failure_info

    @classmethod
    def decode_dict(cls, dic):
        return QueryError(
                message=dic.get("message"),
                sql_state=dic.get("sqlState"),
                error_code=dic.get("errorCode"),
                error_location=ErrorLocation.decode_dict(dic["errorLocation"]) if "errorLocation" in dic else None,
                failure_info=FailureInfo.decode_dict(dic["failureInfo"]) if "failureInfo" in dic else None,
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
                id=dic.get("id"),
                info_uri=dic.get("infoUri"),
                partial_cache_uri=dic.get("partialCancelUri"),
                next_uri=dic.get("nextUri"),
                columns=map(Column.decode_dict, dic["columns"]) if "columns" in dic else None,
                data=dic.get("data"),
                stats=StatementStats.decode_dict(dic["stats"]) if "stats" in dic else None,
                error=QueryError.decode_dict(dic["error"]) if "error" in dic else None,
                )

class PrestoException(Exception):
    pass

class PrestoHttpException(PrestoException):
    def __init__(self, status, message):
        PrestoException.__init__(self, message)
        self.status = status

class PrestoClientException(PrestoException):
    pass

class PrestoQueryException(PrestoException):
    def __init__(self, message, query_id, error_code, failure_info):
        PrestoException.__init__(self, message)
        self.query_id = query_id
        self.error_code = error_code
        self.failure_info = failure_info

class PrestoHeaders(object):
    PRESTO_USER = "X-Presto-User"
    PRESTO_SOURCE = "X-Presto-Source"
    PRESTO_CATALOG = "X-Presto-Catalog"
    PRESTO_SCHEMA = "X-Presto-Schema"
    PRESTO_TIME_ZONE = "X-Presto-Time-Zone"
    PRESTO_LANGUAGE = "X-Presto-Language"

    PRESTO_CURRENT_STATE = "X-Presto-Current-State"
    PRESTO_MAX_WAIT = "X-Presto-Max-Wait"
    PRESTO_MAX_SIZE = "X-Presto-Max-Size"
    PRESTO_PAGE_SEQUENCE_ID = "X-Presto-Page-Sequence-Id"
    PRESTO_SESSION = "X-Presto-Session"

class StatementClient(object):
    HEADERS = {
            "User-Agent": "presto-python/%s" % VERSION
            }

    def __init__(self, http_client, query, **options):
        self.http_client = http_client
        self.query = query
        self.options = options

        self.closed = False
        self.exception = None
        self.results = None
        self._post_query_request()

    def _post_query_request(self, session=None, **options):
        headers = StatementClient.HEADERS.copy()

        if self.options.get("user") is not None:
            headers[PrestoHeaders.PRESTO_USER] = self.options["user"]
        if self.options.get("source") is not None:
            headers[PrestoHeaders.PRESTO_SOURCE] = self.options["source"]
        if self.options.get("catalog") is not None:
            headers[PrestoHeaders.PRESTO_CATALOG] = self.options["catalog"]
        if self.options.get("schema") is not None:
            headers[PrestoHeaders.PRESTO_SCHEMA] = self.options["schema"]
        if self.options.get("time_zone") is not None:
            headers[PrestoHeaders.PRESTO_TIME_ZONE] = self.options["time_zone"]
        if self.options.get("language") is not None:
            headers[PrestoHeaders.PRESTO_LANGUAGE] = self.options["language"]
        session = self.options.get("session")
        if session is not None:
            headers[PrestoHeaders.PRESTO_SESSION] = ','.join([
                str(field) + "=" + str(session[field]) for field in session])

        self.http_client.request("POST", "/v1/statement", self.query, headers)
        response = self.http_client.getresponse()
        body = response.read()

        if response.status != 200:
            raise PrestoHttpException(response.status, "Failed to start query: %s" % body)

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
                self.exception = PrestoHttpException(response.status, "Error fetching next at %s returned %s: %s" % (uri, response.status, body))  # TODO error class
                raise self.exception

            if (time.time() - start) > 2*60*60 or self.closed:
                break

        self.exception = PrestoHttpException(408, "Error fetching next")  # TODO error class
        raise self.exception

    def cancel_leaf_stage(self):
        if self.results.next_uri is not None:
            self.http_client.request("DELETE", self.results.next_uri)
            response = self.http_client.getresponse()
            response.read()
            return response.status / 100 == 2
        return False

    def close(self):
        if self.closed:
            return

        cancel_leaf_stage(self)

        self.closed = True

class Query(object):
    @classmethod
    def start(cls, query, **options):
        http_client = httplib.HTTPConnection(host=options["server"], timeout=options.get("http_timeout", 300))
        return Query(StatementClient(http_client, query, **options))

    def __init__(self, client):
        self.client = client

    def _wait_for_columns(self):
        while self.client.results.columns is None and self.client.advance():
            pass

    def _wait_for_data(self):
        while self.client.results.data is None and self.client.advance():
            pass

    def columns(self):
        self._wait_for_columns()

        if not self.client.is_query_succeeded:
            self._raise_error()

        return self.client.results.columns

    def results(self):
        self._wait_for_data()

        client = self.client

        if not client.is_query_succeeded:
            self._raise_error()

        if self.columns() is None:
            raise PrestoException("Query %s has no columns" % client.results.id)

        while True:
            if client.results.data is None:
                break

            for row in client.results.data:
                yield row

            if not client.advance():
                break

            if client.results.data is None:
                break

    def cancel(self):
        self.client.cancel_leaf_stage()

    def close(self):
        self.client.cancel_leaf_stage()

    def _raise_error(self):
        if self.client.closed:
            raise PrestoClientException("Query aborted by user")
        elif self.client.exception is not None:
            raise self.client.exception
        elif self.client.is_query_failed:
            results = self.client.results
            error = results.error
            if error is None:
                raise PrestoQueryException("Query %s failed: (unknown reason)" % results.id, None, None)
            raise PrestoQueryException("Query %s failed: %s" % (results.id, error.message), results.id, error.error_code, error.failure_info)

class Client(object):
    def __init__(self, **options):
        self.options = options

    def query(self, query):
        return Query.start(query, **self.options)

    def run(self, query):
        q = Query.start(query, **self.options)
        try:
            columns = q.columns()
            if columns is None:
                return [], []
            rows = []
            map(rows.append, q.results())
            return columns, rows
        finally:
            q.close()

