
for this program, we will only be creating seasonal indicator features
    in a raw format. So there is no need to apply cyclical encodings here.
Instead we need well defined binary or integer features that go back to 2010 and 
    goes forward to the current date + 180 days.

# 📅 Calendar-based Features

Day of Week (dow_0–6 one-hot or cyclical sin/cos encoding).

Week of Year (week_num or cyclical).

Month of Year (month_1–12 one-hot or cyclical).

Quarter of Year (q1–q4).


# Year-End Indicator (Nov–Dec, when tax-loss harvesting and portfolio rebalancing are common).

Turn-of-Month Window (dummy for day_of_month ∈ {−2, −1, 0, +1, +2} around month end).

Turn-of-Quarter Window (similar, but for Mar/Jun/Sep/Dec end).

# 🎄 Holiday Seasonality Features

Rather than same-day flags, define windows relative to holidays:

Pre-Holiday Window: 1–10 trading days before holiday (e.g., Thanksgiving, Christmas, July 4, Labor Day).

Post-Holiday Window: 1–5 trading days after.

Holiday Season Indicator: e.g., “Black Friday/Cyber Monday week,” “Christmas rally window (Dec 15–31),” “Santa Rally (last 5 days of year + first 2 of Jan).”

Earnings Season Window: indicator for ±10 trading days around peak earnings weeks (Jan–Feb, Apr–May, Jul–Aug, Oct–Nov).

# 📊 Market Microstructure Seasonality

First Trading Day of Month (binary).

Last Trading Day of Month (binary).

Days Since Holiday (integer counter until next holiday).

Pre-FOMC Window: indicator for 3–5 days before FOMC meetings (monetary policy effect).

Post-FOMC Window: 1–3 days after.

Option Expiration Friday (“Quad Witching”): dummy for Mar/Jun/Sep/Dec third Friday.

# 🔄 Cyclical Encodings (to avoid artificial discontinuities)

For any periodic feature (month, week, day-of-week), encode as:

sin(2π * time_unit / period), cos(2π * time_unit / period)


e.g., sin(2π * month / 12), cos(2π * month / 12).

# 📌 Suggested Implementation Strategy

Start with generic cyclical encodings (month, day-of-week, quarter).

Layer in event-based dummies for pre/post-holiday, turn-of-month, and earnings season.

Expand with macro events like FOMC, option expirations, tax deadlines (Apr 15).