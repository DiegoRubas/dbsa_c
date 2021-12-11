/*-------------------------------------------------------------------------
 *
 * rangetypes_selfuncs.c
 *	  Functions for selectivity estimation of range operators
 *
 * Estimates are based on histograms of lower and upper bounds, and the
 * fraction of empty ranges.
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/rangetypes_selfuncs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/htup_details.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_type.h"
#include "utils/float.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/rangetypes.h"
#include "utils/selfuncs.h"
#include "utils/typcache.h"

static double calc_rangesel(TypeCacheEntry *typcache, VariableStatData *vardata,
							const RangeType *constval, Oid operator);
static double calc_hist_selectivity(TypeCacheEntry *typcache,
									VariableStatData *vardata, const RangeType *constval,
									Oid operator);
static double calc_hist_selectivity_strictly_left_of(TypeCacheEntry *typcache, int *hist_occurs1, int *nbounds1, int *nhist1, RangeBound const_lower, bool *empty);

/*
 * rangesel -- restriction selectivity for range operators
 */
Datum
rangesel(PG_FUNCTION_ARGS)
{
	PlannerInfo *root = (PlannerInfo *) PG_GETARG_POINTER(0);
	Oid			operator = PG_GETARG_OID(1);
	List	   *args = (List *) PG_GETARG_POINTER(2);
	int			varRelid = PG_GETARG_INT32(3);
	VariableStatData vardata;
	Node	   *other;
	bool		varonleft;
	Selectivity selec;
	TypeCacheEntry *typcache = NULL;
	RangeType  *constrange = NULL;

	RangeBound 	const_lower,
				const_upper;
	bool		empty;
	
	if (!get_restriction_variable(root, args, varRelid,
								  &vardata, &other, &varonleft))
		PG_RETURN_FLOAT8(1.0);
	
	if (!IsA(other, Const))
	{
		ReleaseVariableStats(vardata);
		PG_RETURN_FLOAT8(1.0);
	}

	if (((Const *) other)->constisnull)
	{
		ReleaseVariableStats(vardata);
		PG_RETURN_FLOAT8(1.0);
	}
	
	if (!varonleft)
	{
		/* we have other Op var, commute to make var Op other */
		operator = get_commutator(operator);
		if (!operator)
		{
			/* Use default selectivity (should we raise an error instead?) */
			ReleaseVariableStats(vardata);
			PG_RETURN_FLOAT8(1.0);
		}
	}

	if (operator == OID_RANGE_CONTAINS_ELEM_OP)
	{
		typcache = range_get_typcache(fcinfo, vardata.vartype);

		if (((Const *) other)->consttype == typcache->rngelemtype->type_id)
		{
			RangeBound	lower,
						upper;

			lower.inclusive = true;
			lower.val = ((Const *) other)->constvalue;
			lower.infinite = false;
			lower.lower = true;
			upper.inclusive = true;
			upper.val = ((Const *) other)->constvalue;
			upper.infinite = false;
			upper.lower = false;
			constrange = range_serialize(typcache, &lower, &upper, false);
		}
	}
	else if (operator == OID_RANGE_ELEM_CONTAINED_OP)
	{
	}
	else if (((Const *) other)->consttype == vardata.vartype)
	{
		typcache = range_get_typcache(fcinfo, vardata.vartype);

		constrange = DatumGetRangeTypeP(((Const *) other)->constvalue);
	}

	// range_deserialize(typcache, constrange, &const_lower, &const_upper, &empty);

	// printf("%d %d\n", const_lower.val, const_upper.val);
	// fflush(stdout);

	switch (operator)
	{
		case OID_RANGE_OVERLAP_OP:
			printf("range overlap mate\n");
			fflush(stdout);
			break;
		case OID_RANGE_LEFT_OP:
			printf("strictly left of mate\n");
			fflush(stdout);
			break;
	}

	selec = calc_rangesel(typcache, &vardata, constrange, operator);

	ReleaseVariableStats(vardata);

	CLAMP_PROBABILITY(selec);

	PG_RETURN_FLOAT8((float8) selec);
	
}

static double
calc_rangesel(TypeCacheEntry *typcache, VariableStatData *vardata, const RangeType *constval, Oid operator)
{
	double	hist_selec;

	hist_selec = calc_hist_selectivity(typcache, vardata, constval, operator);

	return hist_selec;
}

static double
calc_hist_selectivity(TypeCacheEntry *typcache, VariableStatData *vardata,
					  const RangeType *constval, Oid operator)
{
    AttStatsSlot slot1;
    AttStatsSlot slot1_2;
    AttStatsSlot slot1_3;
    int         i,
				nhist1,
				nbounds1,
				*hist_occurs1;
    Datum		*hist_bounds1;
	
	double		hist_selec;

	RangeBound 	const_lower,
				const_upper;
	bool		empty;

    int         navg1;
    Datum     *avgs1;
	
	// if (!(HeapTupleIsValid(vardata->statsTuple) &&
	// 	  get_attstatsslot(&slot1, vardata->statsTuple,
	// 					   STATISTIC_KIND_BOUNDS_HISTOGRAM, InvalidOid,
	// 					   ATTSTATSSLOT_VALUES)))
	if (!get_attstatsslot(&slot1, vardata->statsTuple,
                             STATISTIC_KIND_OCCURRENCE_HISTOGRAM,
                             InvalidOid, ATTSTATSSLOT_VALUES))
		return -1.0;

    nhist1 = slot1.nvalues;
	// printf("%d\n", nhist1);
	// fflush(stdout);
    hist_occurs1 = (int *) palloc(sizeof(int) * nhist1);
	
	for (i = 0; i < nhist1; i++)
    {
        hist_occurs1[i] = slot1.values[i];
        /* The histogram should not contain any empty ranges */
        if (empty)
            elog(ERROR, "bounds histogram contains an empty range");
    }

    printf("table_1_hist_occurs = [");
    for (i = 0; i < nhist1; i++)
    {
        printf("%d", hist_occurs1[i]);
        if (i < nhist1 - 1)
            printf(", ");
    }
    printf("]\n");

	range_deserialize(typcache, constval, &const_lower, &const_upper, &empty);

	printf("%d %d\n", const_lower.val, const_upper.val);
	fflush(stdout);

	switch (operator)
	{
		case OID_RANGE_LEFT_OP:
			/* var << const when upper(var) < lower(const) */
			hist_selec = calc_hist_selectivity_strictly_left_of(typcache, hist_occurs1, nbounds1, nhist1, const_lower, false);
			break;

		// case OID_RANGE_OVERLAP_OP:

		// 	/*
		// 	 * A && B <=> NOT (A << B OR A >> B).
		// 	 *
		// 	 * Since A << B and A >> B are mutually exclusive events we can
		// 	 * sum their probabilities to find probability of (A << B OR A >>
		// 	 * B).
		// 	 *
		// 	 * "range @> elem" is equivalent to "range && [elem,elem]". The
		// 	 * caller already constructed the singular range from the element
		// 	 * constant, so just treat it the same as &&.
		// 	 */

		// 	hist_selec = calc_hist_selectivity_overlaps(typcache, &const_lower, hist_upper, nhist, false);
		// 	break;
	}

}

static double 
calc_hist_selectivity_strictly_left_of(TypeCacheEntry *typcache, int *hist_occurs1, 
										int *nbounds1, int *nhist1, RangeBound const_lower, bool *empty)
{
	int i = 0;
}
