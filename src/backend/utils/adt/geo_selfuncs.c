/*-------------------------------------------------------------------------
 *
 * geo_selfuncs.c
 *    Selectivity routines registered in the operator catalog in the
 *    "oprrest" and "oprjoin" attributes.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/utils/adt/geo_selfuncs.c
 *
 *  XXX These are totally bogus.  Perhaps someone will make them do
 *  something reasonable, someday.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/builtins.h"
#include "utils/geo_decls.h"

#include "access/htup_details.h"
#include "catalog/pg_statistic.h"
#include "nodes/pg_list.h"
#include "optimizer/pathnode.h"
#include "optimizer/optimizer.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "utils/selfuncs.h"
#include "utils/rangetypes.h"


/*
 *  Selectivity functions for geometric operators.  These are bogus -- unless
 *  we know the actual key distribution in the index, we can't make a good
 *  prediction of the selectivity of these operators.
 *
 *  Note: the values used here may look unreasonably small.  Perhaps they
 *  are.  For now, we want to make sure that the optimizer will make use
 *  of a geometric index if one is available, so the selectivity had better
 *  be fairly small.
 *
 *  In general, GiST needs to search multiple subtrees in order to guarantee
 *  that all occurrences of the same key have been found.  Because of this,
 *  the estimated cost for scanning the index ought to be higher than the
 *  output selectivity would indicate.  gistcostestimate(), over in selfuncs.c,
 *  ought to be adjusted accordingly --- but until we can generate somewhat
 *  realistic numbers here, it hardly matters...
 */


/*
 * Selectivity for operators that depend on area, such as "overlap".
 */

Datum
areasel(PG_FUNCTION_ARGS)
{
    PG_RETURN_FLOAT8(0.005);
}

Datum
areajoinsel(PG_FUNCTION_ARGS)
{
    PG_RETURN_FLOAT8(0.005);
}

/*
 *  positionsel
 *
 * How likely is a box to be strictly left of (right of, above, below)
 * a given box?
 */

Datum
positionsel(PG_FUNCTION_ARGS)
{
    PG_RETURN_FLOAT8(0.1);
}

Datum
positionjoinsel(PG_FUNCTION_ARGS)
{
    PG_RETURN_FLOAT8(0.1);
}

/*
 *  contsel -- How likely is a box to contain (be contained by) a given box?
 *
 * This is a tighter constraint than "overlap", so produce a smaller
 * estimate than areasel does.
 */

Datum
contsel(PG_FUNCTION_ARGS)
{
    PG_RETURN_FLOAT8(0.001);
}

Datum
contjoinsel(PG_FUNCTION_ARGS)
{
    PG_RETURN_FLOAT8(0.001);
}

/*
 * Range Overlaps Join Selectivity.
 */
Datum
rangeoverlapsjoinsel(PG_FUNCTION_ARGS)
{
    PlannerInfo *root = (PlannerInfo *) PG_GETARG_POINTER(0);
    Oid         operator = PG_GETARG_OID(1);
    List       *args = (List *) PG_GETARG_POINTER(2);
    JoinType    jointype = (JoinType) PG_GETARG_INT16(3);
    SpecialJoinInfo *sjinfo = (SpecialJoinInfo *) PG_GETARG_POINTER(4);
    Oid         collation = PG_GET_COLLATION();

    double      selec = 0.005;

    VariableStatData vardata1;
    VariableStatData vardata2;
    Oid         opfuncoid;
    AttStatsSlot sslot1;
    AttStatsSlot sslot2;
    AttStatsSlot sslot3;
    int         nhist;
    RangeBound *hist_lower1;
    RangeBound *hist_upper1;
    float8     *hist_lengths1;
    float8     *hist_occurs1;
    int         i;
    Form_pg_statistic stats1 = NULL;
    TypeCacheEntry *typcache = NULL;
    bool        join_is_reversed;
    bool        empty;


    get_join_variables(root, args, sjinfo,
                       &vardata1, &vardata2, &join_is_reversed);

    typcache = range_get_typcache(fcinfo, vardata1.vartype);
    opfuncoid = get_opcode(operator);

    memset(&sslot1, 0, sizeof(sslot1));

    /* Can't use the histogram with insecure range support functions */
    if (!statistic_proc_security_check(&vardata1, opfuncoid))
        PG_RETURN_FLOAT8((float8) selec);

    if (HeapTupleIsValid(vardata1.statsTuple))
    {
        stats1 = (Form_pg_statistic) GETSTRUCT(vardata1.statsTuple);
        /* Try to get fraction of empty ranges */
        if (!get_attstatsslot(&sslot1, vardata1.statsTuple,
                             STATISTIC_KIND_BOUNDS_HISTOGRAM,
                             InvalidOid, ATTSTATSSLOT_VALUES))
        {
            ReleaseVariableStats(vardata1);
            ReleaseVariableStats(vardata2);
            PG_RETURN_FLOAT8((float8) selec);
        }
        if (!get_attstatsslot(&sslot2, vardata1.statsTuple,
                             STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM,
                             InvalidOid, ATTSTATSSLOT_VALUES))
        {
            ReleaseVariableStats(vardata1);
            ReleaseVariableStats(vardata2);
            PG_RETURN_FLOAT8((float8) selec);
        }
        if (!get_attstatsslot(&sslot3, vardata1.statsTuple,
                             STATISTIC_KIND_OCCURRENCE_HISTOGRAM,
                             InvalidOid, ATTSTATSSLOT_VALUES))
        {
            ReleaseVariableStats(vardata1);
            ReleaseVariableStats(vardata2);
            PG_RETURN_FLOAT8((float8) selec);
        }
    }

    nhist = sslot1.nvalues;
    hist_lower1 = (RangeBound *) palloc(sizeof(RangeBound) * nhist);
    hist_upper1 = (RangeBound *) palloc(sizeof(RangeBound) * nhist);
    hist_lengths1 = (float8 *) palloc(sizeof(float8) * nhist);
    hist_occurs1 = (float8 *) palloc(sizeof(float8) * nhist);

    // for (i = 0; i < nhist; i++)
    // {
    //     range_deserialize(typcache, DatumGetRangeTypeP(sslot1.values[i]),
    //                       &hist_lower1[i], &hist_upper1[i], &empty);
    //     /* The histogram should not contain any empty ranges */
    //     if (empty)
    //         elog(ERROR, "bounds histogram contains an empty range");
    // }

             // bound_hist_values[i] = PointerGetDatum(range_serialize(typcache, &lowers[pos], &uppers[pos], false));
             // length_hist_values[i] = Float8GetDatum(lengths[pos]);

    for (i = 0; i < nhist; i++)
    {
        range_deserialize(typcache, DatumGetRangeTypeP(sslot1.values[i]),
                          &hist_lower1[i], &hist_upper1[i], &empty);
        hist_lengths1[i] = sslot2.values[i];
        hist_occurs1[i] = sslot3.values[i];
        /* The histogram should not contain any empty ranges */
        if (empty)
            elog(ERROR, "bounds histogram contains an empty range");
    }

    printf("hist_lower = [");
    for (i = 0; i < nhist; i++)
    {
        printf("%d", DatumGetInt16(hist_lower1[i].val));
        if (i < nhist - 1)
            printf(", ");
    }
    printf("]\n");
    printf("hist_upper = [");
    for (i = 0; i < nhist; i++)
    {
        printf("%d", DatumGetInt16(hist_upper1[i].val));
        if (i < nhist - 1)
            printf(", ");
    }
    printf("]\n");
    printf("hist_lengths = [");
    for (i = 0; i < nhist; i++)
    {
        printf("%f", DatumGetFloat8(hist_lengths1[i]));
        if (i < nhist - 1)
            printf(", ");
    }
    printf("]\n");
    printf("hist_occurs = [");
    for (i = 0; i < nhist; i++)
    {
        printf("%f", DatumGetFloat8(hist_occurs1[i]));
        if (i < nhist - 1)
            printf(", ");
    }
    printf("]\n");

    fflush(stdout);

    pfree(hist_lower1);
    pfree(hist_upper1);
    pfree(hist_lengths1);
    // pfree(hist_occurs1);

    free_attstatsslot(&sslot1);
    free_attstatsslot(&sslot2);
    // free_attstatsslot(&sslot3);

    ReleaseVariableStats(vardata1);
    ReleaseVariableStats(vardata2);

    CLAMP_PROBABILITY(selec);
    PG_RETURN_FLOAT8((float8) selec);
}
