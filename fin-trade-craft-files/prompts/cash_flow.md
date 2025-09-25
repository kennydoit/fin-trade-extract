create a program called /data_pipeline/transform/transform_cash_flow.py with the following calculations. There are 2 sections. One to create the features and another to normalize them. 

```
SECTION 1

1. Cash Flow Strength & Coverage

Operating Cash Flow Margin (operating_cashflow / net_income)

Free Cash Flow (operating_cashflow - capital_expenditures)

Free Cash Flow to Net Income ( (operating_cashflow - capital_expenditures) / net_income )

Cash Conversion Ratio (operating_cashflow / proceeds_from_operating_activities)

2. Investment Intensity

Capex Ratio (capital_expenditures / operating_cashflow)

Net Investing Cash Flow to OCF (cashflow_from_investment / operating_cashflow)

3. Financing Strategy

Debt Financing Ratio (proceeds_from_issuance_of_long_term_debt_and_capital_securities / cashflow_from_financing)

Equity Financing Ratio ( (proceeds_from_issuance_of_common_stock + proceeds_from_issuance_of_preferred_stock) / cashflow_from_financing )

Shareholder Return Ratio ( (dividend_payout + payments_for_repurchase_of_common_stock) / cashflow_from_financing )

4. Cash Flexibility & Changes

Net Cash Flow (operating_cashflow + cashflow_from_investment + cashflow_from_financing)

Change in Cash to OCF (change_in_cash_and_cash_equivalents / operating_cashflow)

FX Impact Ratio (change_in_exchange_rate / change_in_cash_and_cash_equivalents)

5. Stability & Trends

Cash Flow Volatility = rolling standard deviation or variance of operating_cashflow (or Free Cash Flow)

Cash Flow Growth = period-over-period growth of operating_cashflow (or Free Cash Flow)

Sustainability Ratio = count of periods with positive operating_cashflow out of last N periods


```
SECTION 2
Normalization & Preprocessing

Safe division and clipping (apply everywhere ratios appear)

safe_div(a,b) = (a / (abs(b) + epsilon))

clipped(x, L, U) = min(max(x, L), U)

Rolling, symbol‑wise stats (to avoid leakage)

mean_roll_N(x) = rolling mean of x over last N periods for the same symbol_id, ordered by fiscal_date_ending, excluding current row

std_roll_N(x) = rolling std of x over last N periods for the same symbol_id, excluding current row

median_abs_roll_N(x) = rolling median of abs(x) over last N periods, excluding current row

Sign‑preserving scale transform for skewed cash values (recommended for: operating_cashflow, capital_expenditures, cashflow_from_investment, cashflow_from_financing, Net Cash Flow, Free Cash Flow)

scale_x = (median_abs_roll_N(x) + epsilon)

asinh_scaled_x = asinh( (x / scale_x) )

Z‑scores (use for any feature x when your model prefers standardized inputs)

zscore_N(x) = ( (x - mean_roll_N(x)) / (std_roll_N(x) + epsilon) )

Min‑max within window (if your model benefits from [0,1])

minmax_N(x) = ( (x - min_roll_N(x)) / (max_roll_N(x) - min_roll_N(x) + epsilon) )

Winsorization for ratio stability (before z‑scoring)

winsor(x, p_low, p_high) = clamp x to empirical p_low/p_high percentiles computed within a rolling window per symbol_id

Growth rates (period‑over‑period, symbol‑wise)

growth_pp(x_t) = ( (x_t - x_{t-1}) / (abs(x_{t-1}) + epsilon) )

loglike_growth(x_t) = ( asinh(x_t) - asinh(x_{t-1}) )

EMA smoothing (optional, leakage‑safe if computed with past only)

ema_N(x)t = (alpha * x_t + (1 - alpha) * ema_N(x){t-1}), with (alpha = (2 / (N + 1)))

Normalization suggestions per feature group (mapping to your 1–5 list)

Cash Flow Strength & Coverage

Operating Cash Flow Margin = (operating_cashflow / net_income) → use (safe_div(operating_cashflow, net_income)), then winsor to ([-R, R]), then optionally zscore_N.

Free Cash Flow = (operating_cashflow - capital_expenditures) → first compute FCF_raw, then asinh_scaled_FCF = asinh( (FCF_raw / (median_abs_roll_N(FCF_raw) + epsilon)) ), then zscore_N(asinh_scaled_FCF) if needed.

Free Cash Flow to Net Income = ( (operating_cashflow - capital_expenditures) / net_income ) → use (safe_div( (operating_cashflow - capital_expenditures), net_income )), then winsor and zscore_N.

Cash Conversion Ratio = (operating_cashflow / proceeds_from_operating_activities) → (safe_div(operating_cashflow, proceeds_from_operating_activities)), winsor and zscore_N.

Investment Intensity

Capex Ratio = (capital_expenditures / operating_cashflow) → (safe_div(capital_expenditures, operating_cashflow)), winsor and zscore_N.

Net Investing Cash Flow to OCF = (cashflow_from_investment / operating_cashflow) → (safe_div(cashflow_from_investment, operating_cashflow)), winsor and zscore_N.

Financing Strategy

Debt Financing Ratio = (proceeds_from_issuance_of_long_term_debt_and_capital_securities / cashflow_from_financing) → (safe_div(proceeds_from_issuance_of_long_term_debt_and_capital_securities, cashflow_from_financing)), winsor and zscore_N.

Equity Financing Ratio = ( (proceeds_from_issuance_of_common_stock + proceeds_from_issuance_of_preferred_stock) / cashflow_from_financing ) → (safe_div( (proceeds_from_issuance_of_common_stock + proceeds_from_issuance_of_preferred_stock), cashflow_from_financing )), winsor and zscore_N.

Shareholder Return Ratio = ( (dividend_payout + payments_for_repurchase_of_common_stock) / cashflow_from_financing ) → (safe_div( (dividend_payout + payments_for_repurchase_of_common_stock), cashflow_from_financing )), winsor and zscore_N.

Cash Flexibility & Changes

Net Cash Flow = (operating_cashflow + cashflow_from_investment + cashflow_from_financing) → asinh_scaled on the level, then zscore_N if needed.

Change in Cash to OCF = (change_in_cash_and_cash_equivalents / operating_cashflow) → (safe_div(change_in_cash_and_cash_equivalents, operating_cashflow)), winsor and zscore_N.

FX Impact Ratio = (change_in_exchange_rate / change_in_cash_and_cash_equivalents) → (safe_div(change_in_exchange_rate, change_in_cash_and_cash_equivalents)), winsor and zscore_N.

Stability & Trends

Cash Flow Volatility = std_roll_N(x) for x in {operating_cashflow, Free Cash Flow} → optionally normalize via (zscore_N(std_roll_N(x))) or use (asinh_scaled_x) before computing std to reduce skew.

Cash Flow Growth = growth_pp on x in {operating_cashflow, Free Cash Flow} → compute (growth_pp(x_t)), then winsor and zscore_N.

Sustainability Ratio = (count_positive_roll_N(operating_cashflow) / N) → already in [0,1]; optionally apply (minmax_N) is unnecessary; you can zscore_N across symbols if you need comparability.

Practical defaults (robust and walk‑forward safe)

Set (epsilon) = (1e-9).

For ratios, winsor to ([-5, 5]) or use percentile winsor with (p_low = 0.5%) and (p_high = 99.5%) within rolling windows per symbol_id.

For rolling statistics, typical N choices: (N = 4) for quarterly, (N = 8–12) for multi‑year quarterly context; ensure (report_type) consistency by computing windows within the same report_type.

Prefer asinh scaling on raw cash flow levels before any further stats to stabilize heavy tails.

Always compute windows “using past only” to respect walk‑forward validation.