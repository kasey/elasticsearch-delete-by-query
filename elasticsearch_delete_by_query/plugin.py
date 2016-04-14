from elasticsearch.client.utils import AddonClient, query_params, _make_path, SKIP_IN_PATH


# COPYPASTA ingredient list:
# https://github.com/elastic/elasticsearch-py/blob/1.x/elasticsearch/client/__init__.py#L809
# https://github.com/elastic/elasticsearch-watcher-py/blob/master/elasticsearch_watcher/watcher.py

class DeleteByQuery(AddonClient):
    namespace = 'delete_by_query'

    @query_params('allow_no_indices', 'analyzer', 'consistency',
        'default_operator', 'df', 'expand_wildcards', 'ignore_unavailable', 'q',
        'replication', 'routing', 'timeout')
    def delete_by_query(self, index, doc_type=None, body=None, params=None):
        """
        Delete documents from one or more indices and one or more types based on a query.
        `<http://www.elastic.co/guide/en/elasticsearch/reference/current/docs-delete-by-query.html>`_
        :arg index: A comma-separated list of indices to restrict the operation;
            use `_all` to perform the operation on all indices
        :arg doc_type: A comma-separated list of types to restrict the operation
        :arg body: A query to restrict the operation specified with the Query
            DSL
        :arg allow_no_indices: Whether to ignore if a wildcard indices
            expression resolves into no concrete indices. (This includes `_all`
            string or when no indices have been specified)
        :arg analyzer: The analyzer to use for the query string
        :arg consistency: Specific write consistency setting for the operation
        :arg default_operator: The default operator for query string query (AND
            or OR), default u'OR'
        :arg df: The field to use as default where no field prefix is given in
            the query string
        :arg expand_wildcards: Whether to expand wildcard expression to concrete
            indices that are open, closed or both., default u'open'
        :arg ignore_unavailable: Whether specified concrete indices should be
            ignored when unavailable (missing or closed)
        :arg q: Query in the Lucene query string syntax
        :arg replication: Specific replication type, default 'sync', valid
            choices are: 'sync', 'async'
        :arg routing: Specific routing value
        :arg timeout: Explicit operation timeout
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'index'.")
        _, data = self.transport.perform_request('DELETE', _make_path(index, doc_type, '_query'),
            params=params, body=body)
        return data
