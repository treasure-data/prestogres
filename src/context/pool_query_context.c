/* -*-pgsql-c-*- */
/*
 *
 * $Header$
 *
 * pgpool: a language independent connection pool server for PostgreSQL 
 * written by Tatsuo Ishii
 *
 * Copyright (c) 2003-2014	PgPool Global Development Group
 *
 * Permission to use, copy, modify, and distribute this software and
 * its documentation for any purpose and without fee is hereby
 * granted, provided that the above copyright notice appear in all
 * copies and that both that copyright notice and this permission
 * notice appear in supporting documentation, and that the name of the
 * author not be used in advertising or publicity pertaining to
 * distribution of the software without specific, written prior
 * permission. The author makes no representations about the
 * suitability of this software for any purpose.  It is provided "as
 * is" without express or implied warranty.
 *
 */
#include "pool.h"
#include "pool_config.h"
#include "protocol/pool_proto_modules.h"
#include "utils/palloc.h"
#include "utils/memutils.h"
#include "utils/elog.h"
#include "utils/pool_select_walker.h"
#include "context/pool_session_context.h"
#include "context/pool_query_context.h"
#include "parser/nodes.h"

/* prestogres: prestogres_system_catalog_relcache */
#include "utils/pool_relcache.h"

#include <string.h>
#include <netinet/in.h>
#include <stdlib.h>

/*
 * Where to send query
 */
typedef enum {
	POOL_PRIMARY,
	POOL_STANDBY,
	POOL_EITHER,
	POOL_BOTH
} POOL_DEST;

#define CHECK_QUERY_CONTEXT_IS_VALID \
						do { \
							if (!query_context) \
								ereport(ERROR, \
									(errmsg("setting db node for query to be sent, no query context")));\
						} while (0)

static POOL_DEST send_to_where(Node *node, char *query);
static void where_to_send_deallocate(POOL_QUERY_CONTEXT *query_context, Node *node);
static char* remove_read_write(int len, const char *contents, int *rewritten_len);

/* prestogres: */
static bool match_likely_true_parse_error(const char* query);
static void run_and_rewrite_presto_query(POOL_SESSION_CONTEXT* session_context, POOL_QUERY_CONTEXT* query_context,
		int partial_rewrite_index, bool has_cursor);
static void rewrite_error_query_static(POOL_QUERY_CONTEXT* query_context, const char *message, const char* errcode);
static void replace_ident(char* query, const char* keyword, const char* exact_match, int exact_match_offset, const char* replace);

typedef enum {
	PRESTOGRES_SYSTEM,
	PRESTOGRES_EITHER,
	PRESTOGRES_PRESTO,
	PRESTOGRES_PRESTO_CURSOR,
	PRESTOGRES_BEGIN_COMMIT,
} PRESTOGRES_DEST;
static PRESTOGRES_DEST prestogres_send_to_where(Node *node);

typedef struct {
	const char* prefix;
	const char* suffix;
	char* query;
} partial_rewrite_fragments;
static bool partial_rewrite_presto_query(char* query,
		int partial_rewrite_index, bool has_cursor,
		partial_rewrite_fragments* fragments);

/*
 * Create and initialize per query session context
 */
POOL_QUERY_CONTEXT *pool_init_query_context(void)
{
	POOL_QUERY_CONTEXT *qc;
	MemoryContext oldcontext = MemoryContextSwitchTo(QueryContext);
	qc = palloc0(sizeof(*qc));
	MemoryContextSwitchTo(oldcontext);
	return qc;
}

/*
 * Destroy query context
 */
void pool_query_context_destroy(POOL_QUERY_CONTEXT *query_context)
{
	POOL_SESSION_CONTEXT *session_context;

	if (query_context)
	{
		session_context = pool_get_session_context(false);
		pool_unset_query_in_progress();
		query_context->original_query = NULL;
		session_context->query_context = NULL;
		pfree(query_context);
	}
}

/*
 * Start query
 */
void pool_start_query(POOL_QUERY_CONTEXT *query_context, char *query, int len, Node *node)
{
	POOL_SESSION_CONTEXT *session_context;

	if (query_context)
	{
		MemoryContext old_context;
		session_context = pool_get_session_context(false);
		old_context = MemoryContextSwitchTo(QueryContext);
		query_context->original_length = len;
		query_context->rewritten_length = -1;
		query_context->original_query = pstrdup(query);
		query_context->rewritten_query = NULL;
		query_context->parse_tree = node;
		query_context->virtual_master_node_id = my_master_node_id;
		query_context->is_cache_safe = false;
		query_context->num_original_params = -1;
		if (pool_config->memory_cache_enabled)
			query_context->temp_cache = pool_create_temp_query_cache(query);
		pool_set_query_in_progress();
		session_context->query_context = query_context;
		MemoryContextSwitchTo(old_context);
	}
}

/*
 * Specify DB node to send query
 */
void pool_set_node_to_be_sent(POOL_QUERY_CONTEXT *query_context, int node_id)
{
	CHECK_QUERY_CONTEXT_IS_VALID;

	if (node_id < 0 || node_id >= MAX_NUM_BACKENDS)
		ereport(ERROR,
			(errmsg("setting db node for query to be sent, invalid node id:%d",node_id),
				 errdetail("backend node id: %d out of range, node id can be between 0 and %d",node_id,MAX_NUM_BACKENDS)));

	query_context->where_to_send[node_id] = true;
	
	return;
}

/*
 * Unspecify DB node to send query
 */
void pool_unset_node_to_be_sent(POOL_QUERY_CONTEXT *query_context, int node_id)
{
	CHECK_QUERY_CONTEXT_IS_VALID;

	if (node_id < 0 || node_id >= MAX_NUM_BACKENDS)
		ereport(ERROR,
			(errmsg("un setting db node for query to be sent, invalid node id:%d",node_id),
				 errdetail("backend node id: %d out of range, node id can be between 0 and %d",node_id,MAX_NUM_BACKENDS)));

	query_context->where_to_send[node_id] = false;
	
	return;
}

/*
 * Clear DB node map
 */
void pool_clear_node_to_be_sent(POOL_QUERY_CONTEXT *query_context)
{
	CHECK_QUERY_CONTEXT_IS_VALID;

	memset(query_context->where_to_send, false, sizeof(query_context->where_to_send));
	return;
}

/*
 * Set all DB node map entry
 */
void pool_setall_node_to_be_sent(POOL_QUERY_CONTEXT *query_context)
{
	int i;
	POOL_SESSION_CONTEXT *sc;

	sc = pool_get_session_context(false);

	CHECK_QUERY_CONTEXT_IS_VALID;

	for (i=0;i<NUM_BACKENDS;i++)
	{
		if (private_backend_status[i] == CON_UP ||
			(private_backend_status[i] == CON_CONNECT_WAIT))
		{
			/*
			 * In streaming replication mode, if the node is not
			 * primary node nor load balance node, there's no point to
			 * send query.
			 */
			if (pool_config->master_slave_mode &&
				!strcmp(pool_config->master_slave_sub_mode, MODE_STREAMREP) &&
				i != PRIMARY_NODE_ID && i != sc->load_balance_node_id)
			{
				continue;
			}
			query_context->where_to_send[i] = true;
		}
	}
	return;
}

/*
 * Return true if multiple nodes are targets
 */
bool pool_multi_node_to_be_sent(POOL_QUERY_CONTEXT *query_context)
{
	int i;
	int cnt = 0;

	CHECK_QUERY_CONTEXT_IS_VALID;

	for (i=0;i<NUM_BACKENDS;i++)
	{
		if (((BACKEND_INFO(i)).backend_status == CON_UP ||
			 BACKEND_INFO((i)).backend_status == CON_CONNECT_WAIT) &&
			query_context->where_to_send[i])
		{
			cnt++;
			if (cnt > 1)
			{
				return true;
			}
		}
	}
	return false;
}

/*
 * Return if the DB node is needed to send query
 */
bool pool_is_node_to_be_sent(POOL_QUERY_CONTEXT *query_context, int node_id)
{
	CHECK_QUERY_CONTEXT_IS_VALID;

	if (node_id < 0 || node_id >= MAX_NUM_BACKENDS)
		ereport(ERROR,
			(errmsg("checking if db node is needed to be sent, invalid node id:%d",node_id),
				 errdetail("backend node id: %d out of range, node id can be between 0 and %d",node_id,MAX_NUM_BACKENDS)));

	return query_context->where_to_send[node_id];
}

/*
 * Returns true if the DB node is needed to send query.
 * Intended to be called from VALID_BACKEND
 */
bool pool_is_node_to_be_sent_in_current_query(int node_id)
{
	POOL_SESSION_CONTEXT *sc;

	if (RAW_MODE)
		return node_id == REAL_MASTER_NODE_ID;

	sc = pool_get_session_context(true);
	if (!sc)
		return true;

	if (pool_is_query_in_progress() && sc->query_context)
	{
		return pool_is_node_to_be_sent(sc->query_context, node_id);
	}
	return true;
}

/*
 * Returns virtual master DB node id,
 */
int pool_virtual_master_db_node_id(void)
{
	POOL_SESSION_CONTEXT *sc;

	sc = pool_get_session_context(true);
	if (!sc)
	{
		return REAL_MASTER_NODE_ID;
	}

	if (sc->query_context)
	{
		return sc->query_context->virtual_master_node_id;
	}

	/*
	 * No query context exists.  If in master/slave mode, returns
	 * primary node if exists.  Otherwise returns my_master_node_id,
	 * which represents the last REAL_MASTER_NODE_ID.
	 */
	if (MASTER_SLAVE)
	{
		return PRIMARY_NODE_ID;
	}
	return my_master_node_id;
}

/*
 * Decide where to send queries(thus expecting response)
 */
void pool_where_to_send(POOL_QUERY_CONTEXT *query_context, char *query, Node *node)
{
	POOL_SESSION_CONTEXT *session_context;
	POOL_CONNECTION_POOL *backend;
	int i;

	const char* static_error_message = "";
	enum {
		REWRITE_NONE,
		REWRITE_PRESTO,
		REWRITE_ERROR,
	} prestogres_rewrite_mode = REWRITE_NONE;
	int partial_rewrite_index = -1;
	bool partial_rewrite_has_cursor = false;

	CHECK_QUERY_CONTEXT_IS_VALID;

	/* prestogres: create system catalog */
	prestogres_init_system_catalog();

	session_context = pool_get_session_context(false);
	backend = session_context->backend;

	/*
	 * Zap out DB node map
	 */
	pool_clear_node_to_be_sent(query_context);

	/*
	 * If there is "NO LOAD BALANCE" comment, we send only to master node.
	 */
	/* prestogres: ignore NO LOAD BALANCE comment */
	/*
	if (!strncasecmp(query, NO_LOAD_BALANCE, NO_LOAD_BALANCE_COMMENT_SZ))
	{
		pool_set_node_to_be_sent(query_context,
								 MASTER_SLAVE ? PRIMARY_NODE_ID : REAL_MASTER_NODE_ID);
		for (i=0;i<NUM_BACKENDS;i++)
		{
			if (query_context->where_to_send[i])
			{
				query_context->virtual_master_node_id = i;
				break;
			}
		}
		return;
	}
	*/

	/*
	 * In raw mode, we send only to master node. Simple enough.
	 */
	if (RAW_MODE)
	{
		pool_set_node_to_be_sent(query_context, REAL_MASTER_NODE_ID);
	}
	else if (1)  /* prestogres: enter here regardless of the mode */
	{
		pool_set_node_to_be_sent(query_context, PRIMARY_NODE_ID);
		/*
		 * If failed to parse the statement(s), run it on Presto because
		 * it may include Presto's SQL syntax extensions.
		 */
		if (query_context->is_parse_error && !match_likely_true_parse_error(query_context->original_query))
		{
			ereport(DEBUG1, (errmsg("prestogres: send_to_where: parse error")));
			prestogres_rewrite_mode = REWRITE_PRESTO;
		}

		/* single statement */
		else if (!query_context->is_multi_statement)
		{
			PRESTOGRES_DEST dest = prestogres_send_to_where(node);
			switch (dest) {
			case PRESTOGRES_SYSTEM:
			case PRESTOGRES_EITHER:
				prestogres_rewrite_mode = REWRITE_NONE;
				break;
			case PRESTOGRES_PRESTO:
				prestogres_rewrite_mode = REWRITE_PRESTO;
				break;
			case PRESTOGRES_BEGIN_COMMIT:
				/* single begin/commit */
				prestogres_rewrite_mode = REWRITE_NONE;
				break;
			case PRESTOGRES_PRESTO_CURSOR:
				/* single cursor uses partial rewrite */
				prestogres_rewrite_mode = REWRITE_PRESTO;
				partial_rewrite_index = 0;
				partial_rewrite_has_cursor = true;
				break;
			}
		}

		/* multi statements */
		else
		{
			/* multi statements */
			int presto_query_count = 0;
			int presto_query_index = 0;
			bool has_begin_commit = false;
			bool has_cursor = false;

			PRESTOGRES_DEST merged_dest;
			List *list;
			ListCell *cell;

			ereport(DEBUG1, (errmsg("prestogres: send_to_where: multi-statement")));

			/* parse SQL string again */
			list = raw_parser(query);

			/* for each statement, call prestogres_send_to_where and merge the dest */
			i = 0;
			merged_dest = PRESTOGRES_EITHER;
			foreach(cell, list)
			{
				ereport(DEBUG1, (errmsg("prestogres: send_to_where: multi-statement: statement %d", i)));
				Node* stmt = (Node*) lfirst(cell);
				PRESTOGRES_DEST dest = prestogres_send_to_where(stmt);
				switch (dest) {
				case PRESTOGRES_SYSTEM:
					merged_dest = PRESTOGRES_SYSTEM;
					break;
				case PRESTOGRES_EITHER:
					break;
				case PRESTOGRES_PRESTO_CURSOR:
					has_cursor = true;
					/* pass through */
				case PRESTOGRES_PRESTO:
					if (merged_dest == PRESTOGRES_EITHER) {
						merged_dest = PRESTOGRES_PRESTO;
					}
					presto_query_count++;
					presto_query_index = i;
					break;
				case PRESTOGRES_BEGIN_COMMIT:
					has_begin_commit = true;
					/* keeps PRESTOGRES_EITHER */
					break;
				}
				i++;
			}

			switch (merged_dest) {
			case PRESTOGRES_PRESTO:
				if (has_cursor || has_begin_commit) {
					if (presto_query_count == 1) {
						ereport(DEBUG1, (errmsg("prestogres: send_to_where: multi-statement with partial rewrite")));
						/* use partial rewrite */
						prestogres_rewrite_mode = REWRITE_PRESTO;
						partial_rewrite_index = presto_query_index;
						partial_rewrite_has_cursor = has_cursor;
					} else {
						/* partial rewrite of multiple Presto statements is not supported */
						prestogres_rewrite_mode = REWRITE_ERROR;
						static_error_message = "Running multiple statements on Presto is not supported";
					}
				}
				else
				{
					ereport(DEBUG1, (errmsg("prestogres: send_to_where: multi-statement with entire query rewrite")));
					prestogres_rewrite_mode = REWRITE_PRESTO;
				}
				break;
			case PRESTOGRES_SYSTEM:
			case PRESTOGRES_EITHER:  /* EITHER prefers no rewrite */
			default:
				ereport(DEBUG1, (errmsg("prestogres: send_to_where: multi-statement without rewrite")));
				prestogres_rewrite_mode = REWRITE_NONE;
				break;
			}
		}
	}
	else if (MASTER_SLAVE && query_context->is_multi_statement)
	{
		/*
		 * If we are in master/slave mode and we have multi statement
		 * query, we should send it to primary server only. Otherwise
		 * it is possible to send a write query to standby servers
		 * because we only use the first element of the multi
		 * statement query and don't care about the rest.  Typical
		 * situation where we are bugged by this is, "BEGIN;DELETE
		 * FROM table;END". Note that from pgpool-II 3.1.0
		 * transactional statements such as "BEGIN" is unconditionally
		 * sent to all nodes(see send_to_where() for more details).
		 * Someday we might be able to understand all part of multi
		 * statement queries, but until that day we need this band
		 * aid.
		 */
		if (query_context->is_multi_statement)
		{
			pool_set_node_to_be_sent(query_context, PRIMARY_NODE_ID);
		}
	}
	else if (MASTER_SLAVE)
	{
		POOL_DEST dest;

		dest = send_to_where(node, query);

		ereport(DEBUG1,
			(errmsg("decide where to send the queries"),
				 errdetail("destination = %d for query= \"%s\"", dest, query)));

		/* Should be sent to primary only? */
		if (dest == POOL_PRIMARY)
		{
			pool_set_node_to_be_sent(query_context, PRIMARY_NODE_ID);
		}
		/* Should be sent to both primary and standby? */
		else if (dest == POOL_BOTH)
		{
			pool_setall_node_to_be_sent(query_context);
		}

		/*
		 * Ok, we might be able to load balance the SELECT query.
		 */
		else
		{
			if (pool_config->load_balance_mode &&
				is_select_query(node, query) &&
				MAJOR(backend) == PROTO_MAJOR_V3)
			{
				/* 
				 * If (we are outside of an explicit transaction) OR
				 * (the transaction has not issued a write query yet, AND
				 *	transaction isolation level is not SERIALIZABLE)
				 * we might be able to load balance.
				 */
				if (TSTATE(backend, PRIMARY_NODE_ID) == 'I' ||
					(!pool_is_writing_transaction() &&
					 !pool_is_failed_transaction() &&
					 pool_get_transaction_isolation() != POOL_SERIALIZABLE))
				{
					BackendInfo *bkinfo = pool_get_node_info(session_context->load_balance_node_id);

					/*
					 * Load balance if possible
					 */

					/*
					 * If replication delay is too much, we prefer to send to the primary.
					 */
					if (!strcmp(pool_config->master_slave_sub_mode, MODE_STREAMREP) &&
						pool_config->delay_threshold &&
						bkinfo->standby_delay > pool_config->delay_threshold)
					{
						pool_set_node_to_be_sent(query_context, PRIMARY_NODE_ID);
					}

					/*
					 * If a writing function call is used, 
					 * we prefer to send to the primary.
					 */
					else if (pool_has_function_call(node))
					{
						pool_set_node_to_be_sent(query_context, PRIMARY_NODE_ID);
					}

					/*
					 * If system catalog is used in the SELECT, we
					 * prefer to send to the primary. Example: SELECT
					 * * FROM pg_class WHERE relname = 't1'; Because
					 * 't1' is a constant, it's hard to recognize as
					 * table name.  Most use case such query is
					 * against system catalog, and the table name can
					 * be a temporary table, it's best to query
					 * against primary system catalog.
					 * Please note that this test must be done *before*
					 * test using pool_has_temp_table.
					 */
					else if (pool_has_system_catalog(node))
					{
						pool_set_node_to_be_sent(query_context, PRIMARY_NODE_ID);
					}

					/*
					 * If temporary table is used in the SELECT,
					 * we prefer to send to the primary.
					 */
					else if (pool_config->check_temp_table && pool_has_temp_table(node))
					{
						pool_set_node_to_be_sent(query_context, PRIMARY_NODE_ID);
					}

					/*
					 * If unlogged table is used in the SELECT,
					 * we prefer to send to the primary.
					 */
					else if (pool_config->check_unlogged_table && pool_has_unlogged_table(node))
					{
						pool_set_node_to_be_sent(query_context, PRIMARY_NODE_ID);
					}

					else
					{
						pool_set_node_to_be_sent(query_context,
												 session_context->load_balance_node_id);
					}
				}
				else
				{
					/* Send to the primary only */
					pool_set_node_to_be_sent(query_context, PRIMARY_NODE_ID);
				}
			}
			else
			{
				/* Send to the primary only */
				pool_set_node_to_be_sent(query_context, PRIMARY_NODE_ID);
			}
		}
	}
	else if (REPLICATION || PARALLEL_MODE)
	{
		if (pool_config->load_balance_mode &&
			is_select_query(node, query) &&
			MAJOR(backend) == PROTO_MAJOR_V3)
		{
			/*
			 * If a writing function call is used or replicate_select is true,
			 * we prefer to send to all nodes.
			 */
			if (pool_has_function_call(node) || pool_config->replicate_select)
			{
				pool_setall_node_to_be_sent(query_context);
			}
			/* 
			 * If (we are outside of an explicit transaction) OR
			 * (the transaction has not issued a write query yet, AND
			 *	transaction isolation level is not SERIALIZABLE)
			 * we might be able to load balance.
			 */
			else if (TSTATE(backend, MASTER_NODE_ID) == 'I' ||
					 (!pool_is_writing_transaction() &&
					  !pool_is_failed_transaction() &&
					  pool_get_transaction_isolation() != POOL_SERIALIZABLE))
			{
				/* load balance */
				pool_set_node_to_be_sent(query_context,
										 session_context->load_balance_node_id);
			}
			else
			{
				/* only send to master node */
				pool_set_node_to_be_sent(query_context, REAL_MASTER_NODE_ID);
			}
		}
		else
		{
			if (is_select_query(node, query) && !pool_config->replicate_select &&
				!pool_has_function_call(node))
			{
				/* only send to master node */
				pool_set_node_to_be_sent(query_context, REAL_MASTER_NODE_ID);
			}
			else
			{
				/* send to all nodes */
				pool_setall_node_to_be_sent(query_context);
			}
		}
	}
	else
	{
		ereport(WARNING,
				(errmsg("unknown pgpool-II mode while deciding for where to send query")));
		return;
	}

	/*
	 * EXECUTE?
	 */
	if (IsA(node, ExecuteStmt))
	{
		POOL_SENT_MESSAGE *msg;

		msg = pool_get_sent_message('Q', ((ExecuteStmt *)node)->name);
		if (!msg)
			msg = pool_get_sent_message('P', ((ExecuteStmt *)node)->name);
		if (msg)
			pool_copy_prep_where(msg->query_context->where_to_send,
								 query_context->where_to_send);
	}

	/*
	 * DEALLOCATE?
	 */
	else if (IsA(node, DeallocateStmt))
	{
		where_to_send_deallocate(query_context, node);
	}

	for (i=0;i<NUM_BACKENDS;i++)
	{
		if (query_context->where_to_send[i])
		{
			query_context->virtual_master_node_id = i;
			break;
		}
	}

	/* prestogres query rewrite */
	switch (prestogres_rewrite_mode) {
	case REWRITE_NONE:
		break;

	case REWRITE_PRESTO:
		run_and_rewrite_presto_query(session_context, query_context,
				partial_rewrite_index, partial_rewrite_has_cursor);
		break;

	case REWRITE_ERROR:
		rewrite_error_query_static(query_context, static_error_message, NULL);
		break;
	}

	return;
}

/*
 * Send simple query and wait for response
 * send_type:
 *  -1: do not send this node_id
 *   0: send to all nodes
 *  >0: send to this node_id
 */
POOL_STATUS pool_send_and_wait(POOL_QUERY_CONTEXT *query_context,
							   int send_type, int node_id)
{
	POOL_SESSION_CONTEXT *session_context;
	POOL_CONNECTION *frontend;
	POOL_CONNECTION_POOL *backend;
	bool is_commit;
	bool is_begin_read_write;
	int i;
	int len;
	char *string;

	session_context = pool_get_session_context(false);
	frontend = session_context->frontend;
	backend = session_context->backend;
	is_commit = is_commit_or_rollback_query(query_context->parse_tree);
	is_begin_read_write = false;
	len = 0;
	string = NULL;

	/*
	 * If the query is BEGIN READ WRITE or
	 * BEGIN ... SERIALIZABLE in master/slave mode,
	 * we send BEGIN to slaves/standbys instead.
	 * original_query which is BEGIN READ WRITE is sent to primary.
	 * rewritten_query which is BEGIN is sent to standbys.
	 */
	if (pool_need_to_treat_as_if_default_transaction(query_context))
	{
		is_begin_read_write = true;
	}
	else
	{
		if (query_context->rewritten_query)
		{
			len = query_context->rewritten_length;
			string = query_context->rewritten_query;
		}
		else
		{
			len = query_context->original_length;
			string = query_context->original_query;
		}
	}

	/* Send query */
	for (i=0;i<NUM_BACKENDS;i++)
	{
		if (!VALID_BACKEND(i))
			continue;
		else if (send_type < 0 && i == node_id)
			continue;
		else if (send_type > 0 && i != node_id)
			continue;

		/*
		 * If in master/slave mode, we do not send COMMIT/ABORT to
		 * slaves/standbys if it's in I(idle) state.
		 */
		if (is_commit && MASTER_SLAVE && !IS_MASTER_NODE_ID(i) && TSTATE(backend, i) == 'I')
		{
			pool_unset_node_to_be_sent(query_context, i);
			continue;
		}

		/*
		 * If in reset context, we send COMMIT/ABORT to nodes those
		 * are not in I(idle) state.  This will ensure that
		 * transactions are closed.
		 */
		if (is_commit && session_context->reset_context && TSTATE(backend, i) == 'I')
		{
			pool_unset_node_to_be_sent(query_context, i);
			continue;
		}

		if (is_begin_read_write)
		{
			if (REAL_PRIMARY_NODE_ID == i)
			{
				len = query_context->original_length;
				string = query_context->original_query;
			}
			else
			{
				len = query_context->rewritten_length;
				string = query_context->rewritten_query;
			}
		}

		per_node_statement_log(backend, i, string);

		send_simplequery_message(CONNECTION(backend, i), len, string, MAJOR(backend));
	}

	/* Wait for response */
	for (i=0;i<NUM_BACKENDS;i++)
	{
		if (!VALID_BACKEND(i))
			continue;
		else if (send_type < 0 && i == node_id)
			continue;
		else if (send_type > 0 && i != node_id)
			continue;

#ifdef NOT_USED
		/*
		 * If in master/slave mode, we do not send COMMIT/ABORT to
		 * slaves/standbys if it's in I(idle) state.
		 */
		if (is_commit && MASTER_SLAVE && !IS_MASTER_NODE_ID(i) && TSTATE(backend, i) == 'I')
		{
			continue;
		}
#endif

		if (is_begin_read_write)
		{
			if(REAL_PRIMARY_NODE_ID == i)
				string = query_context->original_query;
			else
				string = query_context->rewritten_query;
		}
        
        wait_for_query_response_with_trans_cleanup(frontend,
                                                   CONNECTION(backend, i),
                                                   MAJOR(backend),
                                                   MASTER_CONNECTION(backend)->pid,
                                                   MASTER_CONNECTION(backend)->key);
        
		/*
		 * Check if some error detected.  If so, emit
		 * log. This is useful when invalid encoding error
		 * occurs. In this case, PostgreSQL does not report
		 * what statement caused that error and make users
		 * confused.
		 */		
		per_node_error_log(backend, i, string, "pool_send_and_wait: Error or notice message from backend: ", true);
	}

	return POOL_CONTINUE;
}

/*
 * Send extended query and wait for response
 * send_type:
 *  -1: do not send this node_id
 *   0: send to all nodes
 *  >0: send to this node_id
 */
POOL_STATUS pool_extended_send_and_wait(POOL_QUERY_CONTEXT *query_context,
										char *kind, int len, char *contents,
										int send_type, int node_id)
{
	POOL_SESSION_CONTEXT *session_context;
	POOL_CONNECTION *frontend;
	POOL_CONNECTION_POOL *backend;
	bool is_commit;
	bool is_begin_read_write;
	int i;
	int str_len;
	int rewritten_len;
	char *str;
	char *rewritten_begin;

	session_context = pool_get_session_context(false);
	frontend = session_context->frontend;
	backend = session_context->backend;
	is_commit = is_commit_or_rollback_query(query_context->parse_tree);
	is_begin_read_write = false;
	str_len = 0;
	rewritten_len = 0;
	str = NULL;
	rewritten_begin = NULL;

	/*
	 * If the query is BEGIN READ WRITE or
	 * BEGIN ... SERIALIZABLE in master/slave mode,
	 * we send BEGIN to slaves/standbys instead.
	 * original_query which is BEGIN READ WRITE is sent to primary.
	 * rewritten_query which is BEGIN is sent to standbys.
	 */
	if (pool_need_to_treat_as_if_default_transaction(query_context))
	{
		is_begin_read_write = true;

		if (*kind == 'P')
			rewritten_begin = remove_read_write(len, contents, &rewritten_len);
	}

	if (!rewritten_begin)
	{	
		str_len = len;
		str = contents;
	}

	/* Send query */
	for (i=0;i<NUM_BACKENDS;i++)
	{
		if (!VALID_BACKEND(i))
			continue;
		else if (send_type < 0 && i == node_id)
			continue;
		else if (send_type > 0 && i != node_id)
			continue;

		/*
		 * If in reset context, we send COMMIT/ABORT to nodes those
		 * are not in I(idle) state.  This will ensure that
		 * transactions are closed.
		 */
		if (is_commit && session_context->reset_context && TSTATE(backend, i) == 'I')
		{
			pool_unset_node_to_be_sent(query_context, i);
			continue;
		}

		if (rewritten_begin)
		{
			if (REAL_PRIMARY_NODE_ID == i)
			{
				str = contents;
				str_len = len;
			}
			else
			{
				str = rewritten_begin;
				str_len = rewritten_len;
			}
		}

		if (pool_config->log_per_node_statement)
		{
			char msgbuf[QUERY_STRING_BUFFER_LEN];
			char *stmt;

			if (*kind == 'P' || *kind == 'E')
			{
				if (query_context->rewritten_query)
				{
					if (is_begin_read_write)
					{
						if (REAL_PRIMARY_NODE_ID == i)
							stmt = query_context->original_query;
						else
							stmt = query_context->rewritten_query;
					}
					else
					{
						stmt = query_context->rewritten_query;
					}
				}
				else
				{
					stmt = query_context->original_query;
				}

				if (*kind == 'P')
					snprintf(msgbuf, sizeof(msgbuf), "Parse: %s", stmt);
				else
					snprintf(msgbuf, sizeof(msgbuf), "Execute: %s", stmt);
			}
			else
			{
				snprintf(msgbuf, sizeof(msgbuf), "%c message", *kind);
			}

			per_node_statement_log(backend, i, msgbuf);
		}

		send_extended_protocol_message(backend, i, kind, str_len, str);
	}

	if (!is_begin_read_write)
	{
		if (query_context->rewritten_query)
			str = query_context->rewritten_query;
		else
			str = query_context->original_query;
	}

	/* Wait for response */
	for (i=0;i<NUM_BACKENDS;i++)
	{
		if (!VALID_BACKEND(i))
			continue;
		else if (send_type < 0 && i == node_id)
			continue;
		else if (send_type > 0 && i != node_id)
			continue;

		/*
		 * If in master/slave mode, we do not send COMMIT/ABORT to
		 * slaves/standbys if it's in I(idle) state.
		 */
		if (is_commit && MASTER_SLAVE && !IS_MASTER_NODE_ID(i) && TSTATE(backend, i) == 'I')
		{
			continue;
		}

		if (is_begin_read_write)
		{
			if (REAL_PRIMARY_NODE_ID == i)
				str = query_context->original_query;
			else
				str = query_context->rewritten_query;
		}

        wait_for_query_response_with_trans_cleanup(frontend,
                                                   CONNECTION(backend, i),
                                                   MAJOR(backend),
                                                   MASTER_CONNECTION(backend)->pid,
                                                   MASTER_CONNECTION(backend)->key);

		/*
		 * Check if some error detected.  If so, emit
		 * log. This is useful when invalid encoding error
		 * occurs. In this case, PostgreSQL does not report
		 * what statement caused that error and make users
		 * confused.
		 */		
		per_node_error_log(backend, i, str, "pool_send_and_wait: Error or notice message from backend: ", true);
	}

	if(rewritten_begin)
        pfree(rewritten_begin);
	return POOL_CONTINUE;
}

/*
 * From syntactically analysis decide the statement to be sent to the
 * primary, the standby or either or both in master/slave+HR/SR mode.
 */
static POOL_DEST send_to_where(Node *node, char *query)

{
/* From storage/lock.h */
#define NoLock					0
#define AccessShareLock			1		/* SELECT */
#define RowShareLock			2		/* SELECT FOR UPDATE/FOR SHARE */
#define RowExclusiveLock		3		/* INSERT, UPDATE, DELETE */
#define ShareUpdateExclusiveLock 4		/* VACUUM (non-FULL),ANALYZE, CREATE
										 * INDEX CONCURRENTLY */
#define ShareLock				5		/* CREATE INDEX (WITHOUT CONCURRENTLY) */
#define ShareRowExclusiveLock	6		/* like EXCLUSIVE MODE, but allows ROW
										 * SHARE */
#define ExclusiveLock			7		/* blocks ROW SHARE/SELECT...FOR
										 * UPDATE */
#define AccessExclusiveLock		8		/* ALTER TABLE, DROP TABLE, VACUUM
										 * FULL, and unqualified LOCK TABLE */

/* From 9.0 include/nodes/node.h */
	static NodeTag nodemap[] = {
		T_PlannedStmt,
		T_InsertStmt,
		T_DeleteStmt,
		T_UpdateStmt,
		T_SelectStmt,
		T_AlterTableStmt,
		T_AlterTableCmd,
		T_AlterDomainStmt,
		T_SetOperationStmt,
		T_GrantStmt,
		T_GrantRoleStmt,
		/*
		T_AlterDefaultPrivilegesStmt,	Our parser does not support yet
		*/
		T_ClosePortalStmt,
		T_ClusterStmt,
		T_CopyStmt,
		T_CreateStmt,	/* CREATE TABLE */
		T_DefineStmt,	/* CREATE AGGREGATE, OPERATOR, TYPE */
		T_DropStmt,		/* DROP TABLE etc. */
		T_TruncateStmt,
		T_CommentStmt,
		T_FetchStmt,
		T_IndexStmt,	/* CREATE INDEX */
		T_CreateFunctionStmt,
		T_AlterFunctionStmt,
		/*
		T_DoStmt,		Our parser does not support yet
		*/
		T_RenameStmt,	/* ALTER AGGREGATE etc. */
		T_RuleStmt,		/* CREATE RULE */
		T_NotifyStmt,
		T_ListenStmt,
		T_UnlistenStmt,
		T_TransactionStmt,
		T_ViewStmt,		/* CREATE VIEW */
		T_LoadStmt,
		T_CreateDomainStmt,
		T_CreatedbStmt,
		T_DropdbStmt,
		T_VacuumStmt,
		T_ExplainStmt,
		T_CreateSeqStmt,
		T_AlterSeqStmt,
		T_VariableSetStmt,		/* SET */
		T_VariableShowStmt,
		T_DiscardStmt,
		T_CreateTrigStmt,
		T_CreatePLangStmt,
		T_CreateRoleStmt,
		T_AlterRoleStmt,
		T_DropRoleStmt,
		T_LockStmt,
		T_ConstraintsSetStmt,
		T_ReindexStmt,
		T_CheckPointStmt,
		T_CreateSchemaStmt,
		T_AlterDatabaseStmt,
		T_AlterDatabaseSetStmt,
		T_AlterRoleSetStmt,
		T_CreateConversionStmt,
		T_CreateCastStmt,
		T_CreateOpClassStmt,
		T_CreateOpFamilyStmt,
		T_AlterOpFamilyStmt,
		T_PrepareStmt,
		T_ExecuteStmt,
		T_DeallocateStmt,		/* DEALLOCATE */
		T_DeclareCursorStmt,	/* DECLARE */
		T_CreateTableSpaceStmt,
		T_DropTableSpaceStmt,
		T_AlterObjectSchemaStmt,
		T_AlterOwnerStmt,
		T_DropOwnedStmt,
		T_ReassignOwnedStmt,
		T_CompositeTypeStmt,	/* CREATE TYPE */
		T_CreateEnumStmt,
		T_AlterTSDictionaryStmt,
		T_AlterTSConfigurationStmt,
		T_CreateFdwStmt,
		T_AlterFdwStmt,
		T_CreateForeignServerStmt,
		T_AlterForeignServerStmt,
		T_CreateUserMappingStmt,
		T_AlterUserMappingStmt,
		T_DropUserMappingStmt,
		/*
		T_AlterTableSpaceOptionsStmt,	Our parser does not support yet
		*/
	};

	if (bsearch(&nodeTag(node), nodemap, sizeof(nodemap)/sizeof(nodemap[0]),
				sizeof(NodeTag), compare) != NULL)
	{
		/*
		 * SELECT INTO
		 * SELECT FOR SHARE or UPDATE
		 */
		if (IsA(node, SelectStmt))
		{
			/* SELECT INTO or SELECT FOR SHARE or UPDATE ? */
			if (pool_has_insertinto_or_locking_clause(node))
				return POOL_PRIMARY;

			return POOL_EITHER;
		}

		/*
		 * COPY FROM
		 */
		else if (IsA(node, CopyStmt))
		{
			return (((CopyStmt *)node)->is_from)?POOL_PRIMARY:POOL_EITHER;
		}

		/*
		 * LOCK
		 */
		else if (IsA(node, LockStmt))
		{
			return (((LockStmt *)node)->mode >= RowExclusiveLock)?POOL_PRIMARY:POOL_BOTH;
		}

		/*
		 * Transaction commands
		 */
		else if (IsA(node, TransactionStmt))
		{
			/*
			 * Check "BEGIN READ WRITE" "START TRANSACTION READ WRITE"
			 */
			if (is_start_transaction_query(node))
			{
				/* But actually, we send BEGIN to standby if it's
				   BEGIN READ WRITE or START TRANSACTION READ WRITE */
				if (is_read_write((TransactionStmt *)node))
					return POOL_BOTH;
				/* Other TRANSACTION start commands are sent to both primary
				   and standby */
				else
					return POOL_BOTH;
			}
			/* SAVEPOINT related commands are sent to both primary and standby */
			else if (is_savepoint_query(node))
				return POOL_BOTH;
			/*
			 * 2PC commands
			 */
			else if (is_2pc_transaction_query(node))
				return POOL_PRIMARY;
			else
				/* COMMIT etc. */
				return POOL_BOTH;
		}

		/*
		 * SET
		 */
		else if (IsA(node, VariableSetStmt))
		{
			ListCell   *list_item;
			bool ret = POOL_BOTH;

			/*
			 * SET transaction_read_only TO off
			 */
			if (((VariableSetStmt *)node)->kind == VAR_SET_VALUE &&
				!strcmp(((VariableSetStmt *)node)->name, "transaction_read_only"))
			{
				List *options = ((VariableSetStmt *)node)->args;
				foreach(list_item, options)
				{
					A_Const *v = (A_Const *)lfirst(list_item);

					switch (v->val.type)
					{
						case T_String:
							if (!strcasecmp(v->val.val.str, "off") ||
								!strcasecmp(v->val.val.str, "f") ||
								!strcasecmp(v->val.val.str, "false"))
								ret = POOL_PRIMARY;
							break;
						case T_Integer:
							if (v->val.val.ival)
								ret = POOL_PRIMARY;
						default:
							break;
					}
				}
				return ret;
			}

			/* SET TRANSACTION ISOLATION LEVEL SERIALIZABLE or
			 * SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE or
			 * SET transaction_isolation TO 'serializable'
			 * SET default_transaction_isolation TO 'serializable'
			 */
			else if (is_set_transaction_serializable(node))
			{
				return POOL_PRIMARY;
			}

			/*
			 * Check "SET TRANSACTION READ WRITE" "SET SESSION
			 * CHARACTERISTICS AS TRANSACTION READ WRITE"
			 */
			else if (((VariableSetStmt *)node)->kind == VAR_SET_MULTI &&
				(!strcmp(((VariableSetStmt *)node)->name, "TRANSACTION") ||
				 !strcmp(((VariableSetStmt *)node)->name, "SESSION CHARACTERISTICS")))
			{
				List *options = ((VariableSetStmt *)node)->args;
				foreach(list_item, options)
				{
					DefElem *opt = (DefElem *) lfirst(list_item);

					if (!strcmp("transaction_read_only", opt->defname))
					{
						bool read_only;

						read_only = ((A_Const *)opt->arg)->val.val.ival;
						if (!read_only)
							return POOL_PRIMARY;
					}
				}
				return POOL_BOTH;
			}
			else
			{
				/*
				 * All other SET command sent to both primary and
				 * standby
				 */
				return POOL_BOTH;
			}
		}

		/*
		 * DISCARD
		 */
		else if (IsA(node, DiscardStmt))
		{
			return POOL_BOTH;
		}

		/*
		 * PREPARE
		 */
		else if (IsA(node, PrepareStmt))
		{
			PrepareStmt *prepare_statement = (PrepareStmt *)node;

			char *string = nodeToString(prepare_statement->query);

			/* Note that this is a recursive call */
			return send_to_where((Node *)(prepare_statement->query), string);
		}

		/*
		 * EXECUTE
		 */
		else if (IsA(node, ExecuteStmt))
		{
			/* This is temporary decision. where_to_send will inherit
			 *  same destination AS PREPARE.
			 */
			return POOL_PRIMARY; 
		}

		/*
		 * DEALLOCATE
		 */
		else if (IsA(node, DeallocateStmt))
		{
			/* This is temporary decision. where_to_send will inherit
			 *  same destination AS PREPARE.
			 */
			return POOL_PRIMARY; 
		}

		/*
		 * Other statements are sent to primary
		 */
		return POOL_PRIMARY;
	}

	/*
	 * All unknown statements are sent to primary
	 */
	return POOL_PRIMARY;
}

static
void where_to_send_deallocate(POOL_QUERY_CONTEXT *query_context, Node *node)
{
	DeallocateStmt *d = (DeallocateStmt *)node;
	POOL_SENT_MESSAGE *msg;

	/* DEALLOCATE ALL? */
	if (d->name == NULL)
	{
		pool_setall_node_to_be_sent(query_context);
	}
	else
	{
		msg = pool_get_sent_message('Q', d->name);
		if (!msg)
			msg = pool_get_sent_message('P', d->name);
		if (msg)
		{
			/* Inherit same map from PREPARE or PARSE */
			pool_copy_prep_where(msg->query_context->where_to_send,
								 query_context->where_to_send);
			return;
		}
		/* prepared statement was not found */
		pool_setall_node_to_be_sent(query_context);
	}
}

/*
 * Returns parse tree for current query.
 * Precondition: the query is in progress state.
 */
Node *pool_get_parse_tree(void)
{
	POOL_SESSION_CONTEXT *sc;

	sc = pool_get_session_context(true);
	if (!sc)
		return NULL;

	if (pool_is_query_in_progress() && sc->query_context)
	{
		return sc->query_context->parse_tree;
	}
	return NULL;
}

/*
 * Returns raw query string for current query.
 * Precondition: the query is in progress state.
 */
char *pool_get_query_string(void)
{
	POOL_SESSION_CONTEXT *sc;

	sc = pool_get_session_context(true);
	if (!sc)
		return NULL;

	if (pool_is_query_in_progress() && sc->query_context)
	{
		return sc->query_context->original_query;
	}
	return NULL;
}

/*
 * Return true if the query is:
 * SET TRANSACTION ISOLATION LEVEL SERIALIZABLE or
 * SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE or
 * SET transaction_isolation TO 'serializable'
 * SET default_transaction_isolation TO 'serializable'
 */
bool is_set_transaction_serializable(Node *node)
{
	ListCell   *list_item;

	if (!IsA(node, VariableSetStmt))
		return false;

	if (((VariableSetStmt *)node)->kind == VAR_SET_VALUE &&
		!strcmp(((VariableSetStmt *)node)->name, "transaction_isolation"))
	{
		List *options = ((VariableSetStmt *)node)->args;
		foreach(list_item, options)
		{
			A_Const *v = (A_Const *)lfirst(list_item);

			switch (v->val.type)
			{
				case T_String:
					if (!strcasecmp(v->val.val.str, "serializable"))
						return true;
					break;
				default:
					break;
			}
		}
		return false;
	}

	else if (((VariableSetStmt *)node)->kind == VAR_SET_MULTI &&
			 (!strcmp(((VariableSetStmt *)node)->name, "TRANSACTION") ||
			  !strcmp(((VariableSetStmt *)node)->name, "SESSION CHARACTERISTICS")))
	{
		List *options = ((VariableSetStmt *)node)->args;
		foreach(list_item, options)
		{
			DefElem *opt = (DefElem *) lfirst(list_item);
			if (!strcmp("transaction_isolation", opt->defname) ||
				!strcmp("default_transaction_isolation", opt->defname))
			{
				A_Const *v = (A_Const *)opt->arg;
 
				if (!strcasecmp(v->val.val.str, "serializable"))
					return true;
			}
		}
	}
	return false;
}

/*
 * Returns true if SQL is transaction starting command (START
 * TRANSACTION or BEGIN)
 */
bool is_start_transaction_query(Node *node)
{
	TransactionStmt *stmt;

	if (node == NULL || !IsA(node, TransactionStmt))
		return false;

	stmt = (TransactionStmt *)node;
	return stmt->kind == TRANS_STMT_START || stmt->kind == TRANS_STMT_BEGIN;
}

/*
 * Return true if start transaction query with "READ WRITE" option.
 */
bool is_read_write(TransactionStmt *node)
{
	ListCell   *list_item;

	List *options = node->options;
	foreach(list_item, options)
	{
		DefElem *opt = (DefElem *) lfirst(list_item);

		if (!strcmp("transaction_read_only", opt->defname))
		{
			bool read_only;

			read_only = ((A_Const *)opt->arg)->val.val.ival;
			if (read_only)
				return false;	/* TRANSACTION READ ONLY */
			else
				/*
				 * TRANSACTION READ WRITE specified. This sounds a little bit strange,
				 * but actually the parse code works in the way.
				 */
				return true;
		}
	}

	/*
	 * No TRANSACTION READ ONLY/READ WRITE clause specified.
	 */
	return false;
}

/*
 * Return true if start transaction query with "SERIALIZABLE" option.
 */
bool is_serializable(TransactionStmt *node)
{
	ListCell   *list_item;

	List *options = node->options;
	foreach(list_item, options)
	{
		DefElem *opt = (DefElem *) lfirst(list_item);

		if (!strcmp("transaction_isolation", opt->defname) &&
			IsA(opt->arg, A_Const) &&
			((A_Const *)opt->arg)->val.type == T_String &&
			!strcmp("serializable", ((A_Const *)opt->arg)->val.val.str))
				return true;
	}
	return false;
}

/*
 * If the query is BEGIN READ WRITE or
 * BEGIN ... SERIALIZABLE in master/slave mode,
 * we send BEGIN to slaves/standbys instead.
 * original_query which is BEGIN READ WRITE is sent to primary.
 * rewritten_query which is BEGIN is sent to standbys.
 */
bool pool_need_to_treat_as_if_default_transaction(POOL_QUERY_CONTEXT *query_context)
{
	return (MASTER_SLAVE &&
			is_start_transaction_query(query_context->parse_tree) &&
			(is_read_write((TransactionStmt *)query_context->parse_tree) ||
			 is_serializable((TransactionStmt *)query_context->parse_tree)));
}

/*
 * Return true if the query is SAVEPOINT related query.
 */
bool is_savepoint_query(Node *node)
{
	if (((TransactionStmt *)node)->kind == TRANS_STMT_SAVEPOINT ||
		((TransactionStmt *)node)->kind == TRANS_STMT_ROLLBACK_TO ||
		((TransactionStmt *)node)->kind == TRANS_STMT_RELEASE)
		return true;

	return false;
}

/*
 * Return true if the query is 2PC transaction query.
 */
bool is_2pc_transaction_query(Node *node)
{
	if (((TransactionStmt *)node)->kind == TRANS_STMT_PREPARE ||
		((TransactionStmt *)node)->kind == TRANS_STMT_COMMIT_PREPARED ||
		((TransactionStmt *)node)->kind == TRANS_STMT_ROLLBACK_PREPARED)
		return true;

	return false;
}

/*
 * Set query state, if a current state is before it than the specified state.
 */
void pool_set_query_state(POOL_QUERY_CONTEXT *query_context, POOL_QUERY_STATE state)
{
	int i;

	CHECK_QUERY_CONTEXT_IS_VALID;

	for (i = 0; i < NUM_BACKENDS; i++)
	{
		if (query_context->where_to_send[i] &&
			statecmp(query_context->query_state[i], state) < 0)
			query_context->query_state[i] = state;
	}
}

/*
 * Return -1, 0 or 1 according to s1 is "before, equal or after" s2 in terms of state
 * transition order. 
 * The State transition order is defined as: UNPARSED < PARSE_COMPLETE < BIND_COMPLETE < EXECUTE_COMPLETE
 */
int statecmp(POOL_QUERY_STATE s1, POOL_QUERY_STATE s2)
{
	int ret;

	switch (s2) {
		case POOL_UNPARSED:
			ret = (s1 == s2) ? 0 : 1;
			break;
		case POOL_PARSE_COMPLETE:
			if (s1 == POOL_UNPARSED)
				ret = -1;
			else
				ret = (s1 == s2) ? 0 : 1;
			break;
		case POOL_BIND_COMPLETE:
			if (s1 == POOL_UNPARSED || s1 == POOL_PARSE_COMPLETE)
				ret = -1;
			else
				ret = (s1 == s2) ? 0 : 1;
			break;
		case POOL_EXECUTE_COMPLETE:
			ret = (s1 == s2) ? 0 : -1;
			break;
		default:
			ret = -2;
			break;
	}

	return ret;
}

/*
 * Remove READ WRITE option from the packet of START TRANSACTION command.
 * To free the return value is required. 
 */
static
char* remove_read_write(int len, const char* contents, int *rewritten_len)
{
	char *rewritten_query;
	char *rewritten_contents;
	const char *name;
	const char *stmt;

	rewritten_query = "BEGIN";
	name = contents;
	stmt = contents + strlen(name) + 1;

	*rewritten_len = len - strlen(stmt) + strlen(rewritten_query);
	if (len < *rewritten_len)
	{
        ereport(ERROR,
            (errmsg("invalid message length of transaction packet")));
	}

	rewritten_contents = palloc(*rewritten_len);

	strcpy(rewritten_contents, name);
	strcpy(rewritten_contents + strlen(name) + 1, rewritten_query);
	memcpy(rewritten_contents + strlen(name) + strlen(rewritten_query) + 2,
		   stmt + strlen(stmt) + 1,
		   len - (strlen(name) + strlen(stmt) + 2));

	return rewritten_contents;
}

/*
 * Return true if current query is safe to cache.
 */
bool pool_is_cache_safe(void)
{
	POOL_SESSION_CONTEXT *sc;

	sc = pool_get_session_context(true);
	if (!sc)
		return false;

	if (pool_is_query_in_progress() && sc->query_context)
	{
		return sc->query_context->is_cache_safe;
	}
	return false;
}

/*
 * Set safe to cache.
 */
void pool_set_cache_safe(void)
{
	POOL_SESSION_CONTEXT *sc;

	sc = pool_get_session_context(true);
	if (!sc)
		return;

	if (sc->query_context)
	{
		sc->query_context->is_cache_safe = true;
	}
}

/*
 * Unset safe to cache.
 */
void pool_unset_cache_safe(void)
{
	POOL_SESSION_CONTEXT *sc;

	sc = pool_get_session_context(true);
	if (!sc)
		return;

	if (sc->query_context)
	{
		sc->query_context->is_cache_safe = false;
	}
}

/*
 * Return true if current temporary query cache is exceeded
 */
bool pool_is_cache_exceeded(void)
{
	POOL_SESSION_CONTEXT *sc;

	sc = pool_get_session_context(true);
	if (!sc)
		return false;

	if (pool_is_query_in_progress() && sc->query_context)
	{
		if (sc->query_context->temp_cache)
			return sc->query_context->temp_cache->is_exceeded;
		return true;
	}
	return false;
}

/*
 * Set current temporary query cache is exceeded
 */
void pool_set_cache_exceeded(void)
{
	POOL_SESSION_CONTEXT *sc;

	sc = pool_get_session_context(true);
	if (!sc)
		return;

	if (sc->query_context && sc->query_context->temp_cache)
	{
		sc->query_context->temp_cache->is_exceeded = true;
	}
}

/*
 * Unset current temporary query cache is exceeded
 */
void pool_unset_cache_exceeded(void)
{
	POOL_SESSION_CONTEXT *sc;

	sc = pool_get_session_context(true);
	if (!sc)
		return;

	if (sc->query_context && sc->query_context->temp_cache)
	{
		sc->query_context->temp_cache->is_exceeded = false;
	}
}

PRESTOGRES_DEST prestogres_send_to_where(Node *node)
{
	/*
	 * SELECT INTO
	 * SELECT FOR SHARE or UPDATE
	 * INSERT INTO ... VALUES
	 * INSERT INTO ... SELECT
	 * CREATE TABLE
	 * CREATE TABLE ... AS SELECT
	 */
	if (IsA(node, SelectStmt) || IsA(node, InsertStmt) || IsA(node, CreateStmt) || IsA(node, CreateTableAsStmt))
	{
		if (pool_has_system_catalog(node))
		{
			ereport(DEBUG1, (errmsg("prestogres_send_to_where: system catalog")));
			return PRESTOGRES_SYSTEM;
		}

		/*
		 * If the statement(s) include black-listend functions,
		 * (black_function_list) run them on PostgreSQL
		 */
		if (pool_has_function_call(node))
		{
			ereport(DEBUG1, (errmsg("prestogres_send_to_where: black-listed functions")));
			return PRESTOGRES_SYSTEM;
		}

		///* SELECT INTO or SELECT FOR SHARE or UPDATE */
		//if (pool_has_insertinto_or_locking_clause(node))
		//{
		//	ereport(DEBUG1, (errmsg("prestogres_send_to_where: INSERT or SELECT with lock options")));
		//	return PRESTOGRES_SYSTEM;
		//}

		/*
		 * statement does not include tables at all (like SELECT 1),
		 */
		if (IsA(node, SelectStmt) && !pool_prestogres_has_relation(node))
		{
			ereport(DEBUG1, (errmsg("prestogres_send_to_where: no relations")));
			return PRESTOGRES_EITHER;
		}

		ereport(DEBUG1, (errmsg("prestogres_send_to_where: select, insert, create table")));
		return PRESTOGRES_PRESTO;
	}

	/*
	 * DECLARE ... CURSOR
	 */
	else if (IsA(node, DeclareCursorStmt))
	{
		ereport(DEBUG1, (errmsg("prestogres_send_to_where: cursor")));
		DeclareCursorStmt * cursor_stmt = (DeclareCursorStmt *)node;
		Node *query = cursor_stmt->query;
		PRESTOGRES_DEST dest = prestogres_send_to_where(query);
		if (dest == PRESTOGRES_PRESTO)
			return PRESTOGRES_PRESTO_CURSOR;
		return dest;
	}

	/*
	 * Cursor-related statements are allowed to be used if
	 * the multi-statement query includes DECLARE ... CURSOR
	 *
	 * FETCH, CLOSE
	 */
	else if (IsA(node, FetchStmt) || IsA(node, ClosePortalStmt))
	{
		ereport(DEBUG1, (errmsg("prestogres_send_to_where: fetch-close")));
		return PRESTOGRES_EITHER;
	}

	/*
	 * EXPLAIN
	 */
	else if (IsA(node, ExplainStmt))
	{
		ereport(DEBUG1, (errmsg("prestogres_send_to_where: explain")));
		// TODO analyze option is not supported but ignores
		ExplainStmt * explain_stmt = (ExplainStmt *)node;
		Node *query = explain_stmt->query;
		return prestogres_send_to_where(query);
	}

	/*
	 * Transaction commands
	 */
	else if (IsA(node, TransactionStmt))
	{
		/*
		 * BEGIN, COMMIT, START TRANSACTION and SAVEPOINT
		 */
		ereport(DEBUG1, (errmsg("prestogres_send_to_where: begin-commit-savepoint")));
		return PRESTOGRES_BEGIN_COMMIT;
	}

	/*
	 * Other statements
	 */
	ereport(DEBUG1, (errmsg("prestogres_send_to_where: others")));
	return PRESTOGRES_SYSTEM;
}

/* prestogres */
#ifndef PRESTO_FETCH_FUNCTION_NAME
#define PRESTO_FETCH_FUNCTION_NAME "presto_fetch"
#endif

#ifndef PRESTO_REWRITE_QUERY_SIZE_LIMIT
#define PRESTO_REWRITE_QUERY_SIZE_LIMIT 32768
#endif

char rewrite_query_string_buffer[PRESTO_REWRITE_QUERY_SIZE_LIMIT];

static char *strcpy_capped(char *buffer, int length, const char *string)
{
	int slen;

	if (buffer == NULL) {
		return NULL;
	}

	slen = strlen(string);
	if (length <= slen) {
		return NULL;
	}

	memcpy(buffer, string, slen + 1);
	return buffer + slen;
}

static char *strcpy_capped_escaped(char *buffer, int length, const char *string, const char *escape_chars)
{
	char *bpos, *bend;
	const char *spos, *send;
	bool escaped = false;

	if (buffer == NULL) {
		return NULL;
	}

	bpos = buffer;
	bend = buffer + length;
	spos = string;
	send = string + strlen(string);
	while (spos < send) {
		if (bpos >= bend) {
			return NULL;
		}

		if (escaped) {
			*bpos = *spos;
			escaped = false;
			spos++;
		} else if (strchr(escape_chars, *spos) != NULL) {
			*bpos = '\\';
			escaped = true;
		} else {
			*bpos = *spos;
			spos++;
		}

		bpos++;
	}

	if (bpos >= bend) {
		return NULL;
	}
	*bpos = '\0';

	return bpos;
}

static void do_replace_query(POOL_QUERY_CONTEXT* query_context, const char *query)
{
	char *dupq = pstrdup(query);

	query_context->original_query = dupq;
	query_context->original_length = strlen(dupq) + 1;
}

static void rewrite_error_query_static(POOL_QUERY_CONTEXT* query_context, const char *message, const char* errcode)
{
	char *buffer, *bufend;

	buffer = rewrite_query_string_buffer;
	bufend = buffer + sizeof(rewrite_query_string_buffer);

	buffer = strcpy_capped(buffer, bufend - buffer, "rollback;do $$ begin raise ");
	buffer = strcpy_capped(buffer, bufend - buffer, "exception '%', E'");
	buffer = strcpy_capped_escaped(buffer, bufend - buffer, message, "'\\$");
	buffer = strcpy_capped(buffer, bufend - buffer, "'");
	if (errcode != NULL) {
		buffer = strcpy_capped(buffer, bufend - buffer, " using errcode = E'");
		buffer = strcpy_capped_escaped(buffer, bufend - buffer, errcode, "'\\$");
		buffer = strcpy_capped(buffer, bufend - buffer, "'");
	}
	buffer = strcpy_capped(buffer, bufend - buffer, "; end $$ language plpgsql");

	if (buffer == NULL) {
		buffer = rewrite_query_string_buffer;
		bufend = buffer + sizeof(rewrite_query_string_buffer);

		buffer = strcpy_capped(buffer, bufend - buffer, "rollback;do $$ begin raise ecxeption 'too long error message'");
		buffer = strcpy_capped(buffer, bufend - buffer, " using errcode = E'");
		buffer = strcpy_capped_escaped(buffer, bufend - buffer, errcode, "'\\$");
		buffer = strcpy_capped(buffer, bufend - buffer, "'; end $$ language plpgsql");

		if (buffer == NULL) {
			do_replace_query(query_context,
					"rollback;do $$ begin raise ecxeption 'too long error message'; end $$ language plpgsql");
			return;
		}
	}

	do_replace_query(query_context, rewrite_query_string_buffer);
}

static void rewrite_error_query(POOL_QUERY_CONTEXT* query_context, char *message, const char* errcode)
{
	/* 20 is for escape characters */
	const size_t static_length = strlen("rollback;do $$ begin raise exception '%', E'' using errcode = E'XXXXX'; end $$ language plpgsql") + 20;

	if (message == NULL) {
		message = "Unknown exception";
	}

	if (errcode == NULL) {
		errcode = "XX000";   /* Internal Error */
	}

	if (sizeof(rewrite_query_string_buffer) < strlen(message) + static_length) {
		message[sizeof(rewrite_query_string_buffer) - static_length - 3] = '.';
		message[sizeof(rewrite_query_string_buffer) - static_length - 2] = '.';
		message[sizeof(rewrite_query_string_buffer) - static_length - 1] = '.';
		message[sizeof(rewrite_query_string_buffer) - static_length - 0] = '\0';
	}

	rewrite_error_query_static(query_context, message, errcode);
}

//#define LIKELY_PARSE_ERROR "\\A(?!.*select).*\\z"

//static pool_regexp_context LIKELY_PARSE_ERROR_REGEXP = {0};

static bool match_likely_true_parse_error(const char* query)
{
	// TODO this is helpful to notice errors to users quickly. But not implemented yet
	/*return prestogres_regexp_match(LIKELY_PARSE_ERROR, &LIKELY_PARSE_ERROR_REGEXP, query);*/
	return false;
}

/*
 * /\A\s*select\s*\*\s*from\s+(("[^\\"]*([\\"][^\\"]*)*")|[a-zA-Z_][a-zA-Z0-9_]*)(\.(("[^\\"]*([\\"][^\\"]*)*")|[a-zA-Z_][a-zA-Z0-9_]*))?\s*(;|\z)/i
 */
#define AUTO_LIMIT_QUERY_PATTERN "\\A\\s*select\\s*\\*\\s*from\\s+((\"[^\\\\\"]*([\\\\\"][^\\\\\"]*)*\")|[a-zA-Z_][a-zA-Z0-9_]*)(\\.((\"[^\\\\\"]*([\\\\\"][^\\\\\"]*)*\")|[a-zA-Z_][a-zA-Z0-9_]*))?\\s*(;|\\z)"

static pool_regexp_context AUTO_LIMIT_REGEXP = {0};

static bool match_auto_limit_pattern(const char* query)
{
	return prestogres_regexp_match(AUTO_LIMIT_QUERY_PATTERN, &AUTO_LIMIT_REGEXP, query);
}

static void run_and_rewrite_presto_query(POOL_SESSION_CONTEXT* session_context, POOL_QUERY_CONTEXT* query_context,
		int partial_rewrite_index, bool has_cursor)
{
	char *buffer, *bufend;
	char *message = NULL, *errcode = NULL;
	partial_rewrite_fragments fragments = {0};
	POOL_SELECT_RESULT *res;
	POOL_CONNECTION *con;
	POOL_CONNECTION_POOL *backend = session_context->backend;
	con = CONNECTION(backend, session_context->load_balance_node_id);
	char* original_query = pstrdup(query_context->original_query);

	/* build start_presto_query */
	buffer = rewrite_query_string_buffer;
	bufend = buffer + sizeof(rewrite_query_string_buffer);

	buffer = strcpy_capped(buffer, bufend - buffer, "select prestogres_catalog.start_presto_query(E'");
	buffer = strcpy_capped_escaped(buffer, bufend - buffer, presto_server, "'\\");
	buffer = strcpy_capped(buffer, bufend - buffer, "', E'");
	buffer = strcpy_capped_escaped(buffer, bufend - buffer, presto_user, "'\\");
	buffer = strcpy_capped(buffer, bufend - buffer, "', E'");
	buffer = strcpy_capped_escaped(buffer, bufend - buffer, presto_catalog, "'\\");
	buffer = strcpy_capped(buffer, bufend - buffer, "', E'");
	buffer = strcpy_capped_escaped(buffer, bufend - buffer, presto_schema, "'\\");
	buffer = strcpy_capped(buffer, bufend - buffer, "', E'");
	buffer = strcpy_capped_escaped(buffer, bufend - buffer, PRESTO_FETCH_FUNCTION_NAME, "'\\");
	buffer = strcpy_capped(buffer, bufend - buffer, "', E'");
	{
		char *query = original_query;

        ereport(DEBUG1,
                (errmsg("run_and_rewrite_presto_query: partial_rewrite_index %d, has_cursor %d",
                        partial_rewrite_index, has_cursor)));
		if (partial_rewrite_index >= 0 || has_cursor) {
			bool rewrote = partial_rewrite_presto_query(original_query, partial_rewrite_index, has_cursor, &fragments);
			if (!rewrote) {
				rewrite_error_query_static(query_context, "failed to rewrite run multi-statement query or cursor query for Presto", NULL);
				return;
			}
			query = fragments.query;
		} else {
			/* not partial rewrite mode. here can't use lexer because query_context->is_parse_error could be true. */
			/* remove last ; */
			pool_regexp_context ctx = {0};
			prestogres_regexp_extract("\\A(.*?);(?:(?:--[^\\n]*\\n)|\\s)*\\z", &ctx, query, 1);
		}

		// TODO query rewriting is necessary because some BI tools assumes PostgreSQL supports INTEGER type
		//      but Presto supports only BIGINT, for example. However, t's hard work to add rewriting rule
		//      here in C as new incompatibility is found. An solution is to run lexer here and pass indexes
		//      of the tokens using int[] type to start_presto_query (PL/Python). We can implement rewriting
		//      rule in Python and easily change them.
		replace_ident(query, "integer", "AS INTEGER)", -3, "bigint");
		// and FLOAT
		replace_ident(query, "float", "AS FLOAT)", -3, "double");

		buffer = strcpy_capped_escaped(buffer, bufend - buffer, query, "'\\");

		if (match_auto_limit_pattern(query)) {
			// TODO send warning message to client
			ereport(DEBUG1, (errmsg("run_and_rewrite_presto_query: adding 'limit 1000' to a SELECT * query")));
			buffer = strcpy_capped(buffer, bufend - buffer, " limit 1000");
		}

		if (fragments.query != NULL)
			pfree(fragments.query);
	}
	buffer = strcpy_capped(buffer, bufend - buffer, "')");

	if (buffer == NULL) {
		rewrite_error_query_static(query_context, "query too long", NULL);
		pfree(original_query);
		return;
	}

	/* run query */
	PG_TRY();
	{
		do_query_or_get_error_message(con,
				rewrite_query_string_buffer, &res, MAJOR(backend), &message, &errcode);
	}
	PG_CATCH();
	{
		rewrite_error_query_static(query_context, "Unknown execution error", NULL);
		pfree(original_query);
		return;
	}
	PG_END_TRY();

	free_select_result(res);

	if (message != NULL || errcode != NULL) {
		rewrite_error_query(query_context, message, errcode);
		return;
	}

	/* rewrite query */
	buffer = rewrite_query_string_buffer;
	bufend = buffer + sizeof(rewrite_query_string_buffer);

	if (fragments.prefix) {
		buffer = strcpy_capped(buffer, bufend - buffer, fragments.prefix);
	}
	buffer = strcpy_capped(buffer, bufend - buffer, "select * from pg_temp." PRESTO_FETCH_FUNCTION_NAME "()");
	if (fragments.suffix) {
		buffer = strcpy_capped(buffer, bufend - buffer, fragments.suffix);
	}

	if (buffer == NULL) {
		rewrite_error_query_static(query_context, "query too long", NULL);
		pfree(original_query);
		return;
	}

	do_replace_query(query_context, rewrite_query_string_buffer);
}

/* these functions are defined at the end of this file to include parser.h */
static void* partial_rewrite_lex_init(char* query);
static void partial_rewrite_lex_destroy(void* lex);
static int partial_rewrite_next_end_of_statement(void* lex, char** pos);
static int partial_rewrite_next_ident(void* lex, char** pos, const char** keyword);

bool partial_rewrite_presto_query(char* query,
		int partial_rewrite_index, bool has_cursor,
		partial_rewrite_fragments* fragments)
{
	char* query_start_pos;
	char* query_suffix_pos;
	const char* keyword = NULL;
	void* lex = partial_rewrite_lex_init(query);

	if (lex == NULL)
		return false;

	/* 1. search beggning of presto query and write \0 */
	fragments->prefix = query;
	while (partial_rewrite_index > 0) {
		/* skip prefix statements ; */
		while (true) {
			int ret = partial_rewrite_next_end_of_statement(lex, &query_start_pos);
			if (ret <= 0) {
				/* rewrite failed */
				goto err;
			} else {
				/* found semicolon. query starts from the next character of ; */
				query_start_pos++;
				break;
			}
		}
		partial_rewrite_index--;
	}
	if (has_cursor) {
		/* if cursor, skip until the beggning of SELECT, INSERT, DELETE, UPDATE, CREATE, or EXECUTE */
		while (true) {
			int ret = partial_rewrite_next_ident(lex, &query_start_pos, &keyword);
			if (ret <= 0) {
				/* rewrite failed */
				goto err;
			} else if (
					strcmp(keyword, "select") == 0 || strcmp(keyword, "insert") == 0 ||
					strcmp(keyword, "delete") == 0 || strcmp(keyword, "update") == 0 ||
					strcmp(keyword, "create") == 0 || strcmp(keyword, "execute") == 0)
			{
				/* query starts at this position */
				break;
			}
			/*
			 * CURSOR ... FOR WITH ... AS SELECT
			 * or
			 * CURSOR ... WITH option FOR SELECT ...
			 */
			else if (strcmp(keyword, "with") == 0)
			{
				char* pos = NULL;
				while (true) {
					int ret = partial_rewrite_next_ident(lex, &pos, &keyword);
					if (ret <= 0) {
						goto err;
					} else if (strcmp(keyword, "as") == 0) {
						/* with-select */
						break;
					} else if (strcmp(keyword, "for") == 0) {
						/* cursor-with-option */
						pos = NULL;
						break;
					}
				}
				if (pos)
					break;
			}
		}
	}

	/* 2. search end of presto query */
	while (true) {
		int ret = partial_rewrite_next_end_of_statement(lex, &query_suffix_pos);
		if (ret < 0) {
			/* rewrite failed */
			goto err;
		} else if (ret == 0) {
			/* end of statement */
			query_suffix_pos = NULL;
			break;
		} else {
			/* found ; */
			break;
		}
	}

	/* 3. create the result */
	if (query_suffix_pos) {
		fragments->query = palloc(query_suffix_pos - query_start_pos + 1);
		strlcpy(fragments->query, query_start_pos, query_suffix_pos - query_start_pos + 1);
		fragments->suffix = query_suffix_pos;
	} else {
		fragments->query = pstrdup(query_start_pos);
	}
	*query_start_pos = '\0';  /* for prefix */

	partial_rewrite_lex_destroy(lex);
	return true;

err:
	partial_rewrite_lex_destroy(lex);
	return false;
}

bool prestogres_regexp_match(const char* regexp, pool_regexp_context* context, const char* string)
{
	int ret;
	int ovec[10];

	if (context->errptr != NULL) {
		return false;
	}

	if (context->pattern == NULL) {
		pcre* pattern;
		pattern = pcre_compile(regexp, PCRE_CASELESS | PCRE_NO_AUTO_CAPTURE | PCRE_UTF8,
				&context->errptr, &context->erroffset, NULL);
		if (pattern == NULL) {
			ereport(ERROR, (errmsg("regexp_match: invalid regexp %s at %d", context->errptr, context->erroffset)));
			return false;
		}
		context->pattern = pattern;
		context->errptr = NULL;

		// TODO pcre_study?
	}

	ret = pcre_exec(context->pattern, NULL, string, strlen(string), 0, 0, ovec, sizeof(ovec));
	if (ret < 0) {
		// error. pattern didn't match in most of cases
		return false;
	}

	return true;
}

bool prestogres_regexp_extract(const char* regexp, pool_regexp_context* context, char* string, int number)
{
	int ret;
	int ovec[10];
	const char* pos;

	if (context->errptr != NULL) {
		return false;
	}

	if (context->pattern == NULL) {
		pcre* pattern;
		pattern = pcre_compile(regexp, PCRE_CASELESS | PCRE_UTF8 | PCRE_DOTALL,
				&context->errptr, &context->erroffset, NULL);
		if (pattern == NULL) {
			ereport(ERROR, (errmsg("regexp_match: invalid regexp %s at %d", context->errptr, context->erroffset)));
			return false;
		}
		context->pattern = pattern;
		context->errptr = NULL;

		// TODO pcre_study?
	}

	ret = pcre_exec(context->pattern, NULL, string, strlen(string), 0, 0, ovec, sizeof(ovec));
	if (ret < 0) {
		// error. pattern didn't match in most of cases
		return false;
	}

	ret = pcre_get_substring(string, ovec, ret, number, &pos);
	if (ret < 0) {
		// number-th group does not match
		return false;
	}

	strlcpy(string, pos, ret+1);
	pcre_free_substring(pos);
	return true;
}

static POOL_RELCACHE *prestogres_system_catalog_relcache;

void prestogres_init_system_catalog()
{
	POOL_CONNECTION_POOL *backend;

	if (!prestogres_system_catalog_relcache)
	{
		char *buffer, *bufend;

		buffer = rewrite_query_string_buffer;
		bufend = buffer + sizeof(rewrite_query_string_buffer);

		buffer = strcpy_capped(buffer, bufend - buffer, "select 1 from (select prestogres_catalog.setup_system_catalog(E'");
		buffer = strcpy_capped_escaped(buffer, bufend - buffer, presto_server, "'\\");
		buffer = strcpy_capped(buffer, bufend - buffer, "', E'");
		buffer = strcpy_capped_escaped(buffer, bufend - buffer, presto_user, "'\\");
		buffer = strcpy_capped(buffer, bufend - buffer, "', E'");
		buffer = strcpy_capped_escaped(buffer, bufend - buffer, presto_catalog, "'\\");
		buffer = strcpy_capped(buffer, bufend - buffer, "', E'");
		buffer = strcpy_capped_escaped(buffer, bufend - buffer, presto_schema, "'\\");
		buffer = strcpy_capped(buffer, bufend - buffer, "', E'");
		buffer = strcpy_capped_escaped(buffer, bufend - buffer, pool_user, "'\\");
		buffer = strcpy_capped(buffer, bufend - buffer, "')");
		buffer = strcpy_capped(buffer, bufend - buffer, ") s");
		buffer = strcpy_capped(buffer, bufend - buffer, ";");

		if (buffer == NULL) {
			ereport(ERROR, (errmsg("prestogres: presto_server, presto_user, presto_catalog or presto_schema is too long")));
			return;
		}

		prestogres_system_catalog_relcache =
				pool_create_relcache(1, rewrite_query_string_buffer,
									int_register_func, int_unregister_func, true);
		if (prestogres_system_catalog_relcache == NULL)
		{
			ereport(WARNING,
					(errmsg("prsetogres: unable to create relcache, while creating tables")));
			return;
		}
	}

	backend = pool_get_session_context(false)->backend;
	pool_search_relcache(prestogres_system_catalog_relcache, backend, "prestogres_catalog");
}

void prestogres_discard_system_catalog()
{
	if (prestogres_system_catalog_relcache)
	{
		pool_discard_relcache(prestogres_system_catalog_relcache);
		prestogres_system_catalog_relcache = NULL;
	}
}

/* necessary to include parser/gram.h */
#ifdef CONNECTION
#undef CONNECTION
#endif

/* from parser/parser.c */
#include "parser/pool_parser.h"
#include "parser/gramparse.h"
#include "parser/gram.h"
#include "parser/parser.h"

typedef struct {
	core_yyscan_t yyscanner;
	base_yy_extra_type yyextra;
	YYSTYPE lval;
	YYLTYPE lloc;
	char* query;
} lex_type;

void* partial_rewrite_lex_init(char* query)
{
	/* copied from parser/parser.c */
	lex_type* lexp = (lex_type*) palloc(sizeof(lex_type));
	memset(&lexp->lval, 0, sizeof(lexp->lval));
	lexp->lloc = 0;
	lexp->query = query;

	lexp->yyscanner = scanner_init(query, &lexp->yyextra.core_yy_extra,
							 ScanKeywords, NumScanKeywords);
	lexp->yyextra.have_lookahead = false;

	return lexp;
}

void partial_rewrite_lex_destroy(void* lex)
{
	lex_type* lexp = (lex_type*) lex;
	scanner_finish(lexp->yyscanner);
}

static bool is_ident(int yychar)
{
	/* See parser/kwlist.h */
	return ABORT_P <= yychar && yychar <= ZONE;
}

int partial_rewrite_next_ident(void* lex, char** pos, const char** keyword)
{
	int yychar;

	lex_type* lexp = (lex_type*) lex;
	while (true) {
		yychar = base_yylex(&lexp->lval, &lexp->lloc, lexp->yyscanner);
		if (yychar <= 0)
			return 0;
		if (yychar == ';')
			return 0;
		if (is_ident(yychar))
		{
			*pos = lexp->query + lexp->lloc;
			*keyword = lexp->lval.keyword;
			return 1;
		}
	}
}

int partial_rewrite_next_end_of_statement(void* lex, char** pos)
{
	int yychar;

	lex_type* lexp = (lex_type*) lex;
	while (true) {
		yychar = base_yylex(&lexp->lval, &lexp->lloc, lexp->yyscanner);
		if (yychar <= 0)
			return 0;
		if (yychar == ';')
		{
			*pos = lexp->query + lexp->lloc;
			return 1;
		}
	}
}

static bool is_keyword_match(const char* query,
		lex_type* lexp, int pos,
		const char* keyword, const char* exact_match, int exact_match_offset)
{
	if (lexp->lval.keyword == NULL || lexp->lval.str == NULL)
		return false;
	if (strcmp(keyword, lexp->lval.keyword) != 0)
		return false;

	if (exact_match)
	{
		int exact_match_pos = lexp->lloc + exact_match_offset;
		if (exact_match_pos < 0)
			return false;
		if (strlen(query) < exact_match_pos + strlen(exact_match))
			return false;
		return memcmp(query + exact_match_pos, exact_match, strlen(exact_match)) == 0;
	} else {
		return true;
	}
}

void replace_ident(char* query, const char* keyword, const char* exact_match, int exact_match_offset, const char* replace)
{
	int yychar;
	int last_copied = 0;
	StringInfoData str = {0};

	initStringInfo(&str);

	lex_type* lexp = (lex_type*) partial_rewrite_lex_init(query);
	while (true) {
		yychar = base_yylex(&lexp->lval, &lexp->lloc, lexp->yyscanner);
		if (yychar <= 0)
		{
			if (last_copied == 0) {
				/* not replaced */
				partial_rewrite_lex_destroy(lexp);
			} else {
				appendBinaryStringInfo(&str, query + last_copied, lexp->lloc - last_copied);
				last_copied = lexp->lloc;
				partial_rewrite_lex_destroy(lexp);
				memcpy(query, str.data, strlen(str.data) + 1);  // TODO size
			}
			pfree(str.data);
			return;
		}
		else if (is_ident(yychar) && is_keyword_match(query, lexp, lexp->lloc, keyword,
					exact_match, exact_match_offset))
		{
			appendBinaryStringInfo(&str, query + last_copied, lexp->lloc - last_copied);
			last_copied = lexp->lloc;
			appendStringInfoString(&str, replace);
			// TODO add space?
			last_copied += strlen(lexp->lval.keyword);
		}
	}
}
