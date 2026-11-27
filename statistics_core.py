"""Direct age standardisation with approximate inference for the synthetic lab."""
from math import erfc, sqrt


def standardised_gap(rows):
    """Calculate domestic minus international in percentage points."""
    by_group = {(row["cohort"], row["age_band"]): row for row in rows}
    bands = sorted({row["age_band"] for row in rows})
    cohorts = ("Domestic", "International")
    population = sum(row["eligible"] for row in rows)
    gap, variance = 0.0, 0.0
    for band in bands:
        paired = [by_group.get((cohort, band)) for cohort in cohorts]
        if any(row is None or min(row["positive"], row["observed"] - row["positive"]) < 5 for row in paired):
            raise ValueError(f"Insufficient observed counts in age band {band}.")
        domestic, international = paired
        weight = sum(row["eligible"] for row in paired) / population
        rates = [row["positive"] / row["observed"] for row in paired]
        gap += weight * (rates[0] - rates[1])
        variance += weight**2 * sum(
            rate * (1 - rate) / row["observed"]
            for rate, row in zip(rates, (domestic, international))
        )
    if not bands:
        raise ValueError("No cohort data.")
    se = sqrt(variance)
    crude = []
    for cohort in cohorts:
        group = [row for row in rows if row["cohort"] == cohort]
        crude.append(sum(row["positive"] for row in group) / sum(row["observed"] for row in group))
    return {
        "crude_gap_pp": round(100 * (crude[0] - crude[1]), 2),
        "adjusted_gap_pp": round(100 * gap, 2),
        "ci95_pp": [round(100 * bound, 2) for bound in (gap - 1.96 * se, gap + 1.96 * se)],
        "p_value_unadjusted": erfc(abs(gap) / (sqrt(2) * se)),
    }
