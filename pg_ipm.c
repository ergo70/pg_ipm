/*-------------------------------------------------------------------------
 *
 * pg_sentinel.c
 *
 * Loadable PostgreSQL module to abort queries if a certain sentinel value
 * is SELECTed. E.g. in case of a SQL injection attack.
 *
 * Copyright 2022 Ernst-Georg Schmid
 *
 * Distributed under The PostgreSQL License
 * see License file for terms
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "executor/executor.h"
#include "access/xact.h"
#include "utils/memutils.h"
#include "utils/builtins.h"
#include "utils/guc.h"

PG_MODULE_MAGIC;

static bool abort_statement_only;
static int relation_oid;
static int col_no;
static int elevel;

static ExecutorRun_hook_type prev_ExecutorRun_hook = NULL;

static void sentinel_ExecutorRun(QueryDesc *queryDesc,
                                 ScanDirection direction, uint64 count, bool execute_once);

void		_PG_init(void);
void		_PG_fini(void);

static void
ExecutePlan(EState *estate,
            PlanState *planstate,
            bool use_parallel_mode,
            CmdType operation,
            bool sendTuples,
            uint64 numberTuples,
            ScanDirection direction,
            DestReceiver *dest)
{
    TupleTableSlot *slot;
    int32 col_val;
    Datum datum;
    uint64 current_tuple_count;
    bool isnull;

    /*
     * initialize local variables
     */
    current_tuple_count = 0;

    /*
     * Set the direction.
     */
    estate->es_direction = direction;

    /*
     * If a tuple count was supplied, we must force the plan to run without
     * parallelism, because we might exit early.
     */
    if (numberTuples)
        use_parallel_mode = false;

    /*
     * If a tuple count was supplied, we must force the plan to run without
     * parallelism, because we might exit early.
     */
    if (use_parallel_mode)
        EnterParallelMode();

    srand(time(NULL));

    rand();

    /*
     * Loop until we've processed the proper number of tuples from the plan.
     */
    for (;;)
    {
        /* Reset the per-output-tuple exprcontext */
        ResetPerTupleExprContext(estate);

        /*
         * Execute the plan and obtain a tuple
         */
        slot = ExecProcNode(planstate);

        /*
         * if the tuple is null, then we assume there is nothing more to
         * process so we just end the loop...
         */
        if (TupIsNull(slot))
        {
            /* Allow nodes to release or shut down resources. */
            (void) ExecShutdownNode(planstate);
            break;
        }

        /*
         * If we have a junk filter, then project a new tuple with the junk
         * removed.
         *
         * Store this new "clean" tuple in the junkfilter's resultSlot.
         * (Formerly, we stored it back over the "dirty" tuple, which is WRONG
         * because that tuple slot has the wrong descriptor.)
         */
        if (estate->es_junkFilter != NULL)
            slot = ExecFilterJunk(estate->es_junkFilter, slot);

        /*
         * Count tuples processed, if this is a SELECT.  (For other operation
         * types, the ModifyTable plan node must count the appropriate
         * events.)
         */
        if (operation == CMD_SELECT)
        {
            if(slot->tts_tableOid == relation_oid)
            {
                datum = slot_getattr(slot, col_no, &isnull);
                col_val = DatumGetInt32(datum);

                //printf("%d\n", col_val);

                col_val += (rand() % (5 - (-5) + 1)) + (-5);

                slot->tts_values[col_no-1] = Int32GetDatum(col_val);

                //printf("%d\n", DatumGetInt32(slot->tts_values[col_no-1]));
            }


            (estate->es_processed)++;
        }

        /*
         * If we are supposed to send the tuple somewhere, do so. (In
         * practice, this is probably always the case at this point.)
         */
        if (sendTuples)
        {
            /*
             * If we are not able to send the tuple, we assume the destination
             * has closed and no more tuples can be sent. If that's the case,
             * end the loop.
             */
            if (!((*dest->receiveSlot) (slot, dest)))
                break;
        }

        /*
         * check our tuple count.. if we've processed the proper number then
         * quit, else loop again and process more tuples.  Zero numberTuples
         * means no limit.
         */
        current_tuple_count++;

        if (numberTuples && numberTuples == current_tuple_count)
            break;
    }

    /*
      * If we know we won't need to back up, we can release resources at this
      * point.
      */
     if (!(estate->es_top_eflags & EXEC_FLAG_BACKWARD))
         ExecShutdownNode(planstate);

    if (use_parallel_mode)
        ExitParallelMode();
}


/*
 * Module load callback
 */
void
_PG_init(void)
{
    /* Define custom GUC variable. */
    DefineCustomIntVariable("pg_sentinel.relation_oid",
                            "Selects the table by Oid "
                            "that contains the sentinel value.",
                            "Oid can be determinded with: SELECT '<schema>.<tablename>'::regclass::oid;",
                            &relation_oid,
                            0,
                            0, INT_MAX,
                            PGC_POSTMASTER,
                            0, /* no flags required */
                            NULL,
                            NULL,
                            NULL);

    /* Define custom GUC variable. */
    DefineCustomIntVariable("pg_sentinel.column_no",
                            "Sets the column position in the table "
                            "which contains the sentinel value.",
                            "Column position can be determined by: SELECT ordinal_position FROM information_schema.columns WHERE table_name='<tablename>' AND column_name = '<column_name>';",
                            &col_no,
                            0,
                            0, INT_MAX,
                            PGC_POSTMASTER,
                            0, /* no flags required */
                            NULL,
                            NULL,
                            NULL);

    /* install the hook */
    prev_ExecutorRun_hook = ExecutorRun_hook;
    ExecutorRun_hook = sentinel_ExecutorRun;

    if (abort_statement_only)
    {
        elevel = ERROR;
    }
    else
    {
        elevel = FATAL;
    }
}

/*
 * Module unload callback
 */
void
_PG_fini(void)
{
    /* Uninstall hook. */
    ExecutorRun_hook = prev_ExecutorRun_hook;
}

void
sentinel_ExecutorRun(QueryDesc *queryDesc,
                     ScanDirection direction, uint64 count,bool execute_once)
{
    EState	   *estate;
    CmdType		operation;
    DestReceiver *dest;
    bool		sendTuples;
    MemoryContext oldcontext;

    /* sanity checks */
    Assert(queryDesc != NULL);

    estate = queryDesc->estate;

    Assert(estate != NULL);
    Assert(!(estate->es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY));

    /*
     * Switch into per-query memory context
     */
    oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

    /* Allow instrumentation of Executor overall runtime */
    if (queryDesc->totaltime)
        InstrStartNode(queryDesc->totaltime);

    /*
     * extract information from the query descriptor and the query feature.
     */
    operation = queryDesc->operation;
    dest = queryDesc->dest;

    /*
     * startup tuple receiver, if we will be emitting tuples
     */
    estate->es_processed = 0;

    sendTuples = (operation == CMD_SELECT ||
                  queryDesc->plannedstmt->hasReturning);

    if (sendTuples)
        (*dest->rStartup) (dest, operation, queryDesc->tupDesc);

    /*
     * run plan
     */
    if (!ScanDirectionIsNoMovement(direction))
        ExecutePlan(estate,
                    queryDesc->planstate,
                    queryDesc->plannedstmt->parallelModeNeeded,
                    operation,
                    sendTuples,
                    count,
                    direction,
                    dest);

    /*
     * shutdown tuple receiver, if we started it
     */
    if (sendTuples)
        (*dest->rShutdown) (dest);

    if (queryDesc->totaltime)
        InstrStopNode(queryDesc->totaltime, estate->es_processed);

    MemoryContextSwitchTo(oldcontext);
}
