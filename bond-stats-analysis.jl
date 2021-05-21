# %%
using DataFrames
using Printf
using CSV
using Dates
using DayCounts
# using Revise
using VegaLite
using Latexify

using Distributed
# if nprocs() == 1
#     addprocs(11)
# end
# @everywhere main_path = "/home/artur/BondPricing/bond-data"
# @everywhere modules_path = string(main_path, "modules")
# @everywhere script_path = string(main_path, "/data-scripts")
# @everywhere include(string(joinpath(modules_path, "data_module"), ".jl"))
# @everywhere include(string(joinpath(modules_path, "stats_module"), ".jl"))
# @everywhere include(string(joinpath(modules_path, "plot_module"), ".jl"))

main_path = "/home/artur/BondPricing/bond-data"
modules_path = string(main_path, "/modules")
script_path = string(main_path, "/data-scripts")
include(string(joinpath(modules_path, "data_module"), ".jl"))
include(string(joinpath(modules_path, "stats_module"), ".jl"))
include(string(joinpath(modules_path, "plot_module"), ".jl"))

ENV["LINES"] = 100
ENV["COLUMNS"] = 1000
# ==============================================================================

# %% markdown
## FILTERING THE MERGENT FISD
1. Indicators:
    - Corporate bonds
    - USD
    - MTN
    - Subsequent Data
    - Covenant Data: (at least one covenant column is non-missing)
2. Create Variable "selected"
    - non_mtn_selected: usd, corp bond, not mtn, (cov_data != false & subsequent data != false)
    - mtn_selected: usd, corp bond, mtn, cov_data == true
    - selected: non_mtn_selected OR mtn_selected
3. Convertible bonds
    - true if any col in convertible cols lists is non-missing and different from "N"
4. Create Covenant Groups Indicators
5. Covenant Indicator: true if any covenant group indicator is true.

## Merge TRACE and MERGENT FISD
1. Merge datasets:
    - Merge on CUSIP
    - For each trade execution date, find the last Rating date
    - Drop if no rating date
2. Create Statistics Variables
    - IG indicator
    - Age and TTM
    - Trade Size Categories
    - ATS indicator (adjust)
3. After the merging process:
    - `:all_sample` indicator: usd, corp bond
    - `:trd_exctn_qtr`: Trade Execution Quarter
4. Create Year/Quarter indicator
5. Select Columns:
    - ID
    - Volume
    - Indicators:
        - All Sample (USD Corp Bonds)
        - IG
        - Covenant
        - ATS
6. Filter All US Corp Bonds
    - Yr/Qtr + All Sample + Selected Cols
7. Filter Non-MTN US Corp Bonds w/ covenant Info Available
    - Year/Quarter indicators
    - Keep only the selected securities
        - See section above: usd, corp bonds, with covenant data available
    - Create indicator for bonds/issuers that trade
      on both markets in the same period
8. Compute Statistics
    1. All US Corp Bonds
        1. Identify combinations:
            - Secondary Bond Market (SBM): ATS, OTC or Both
            - Credit Rating: IG, HY or Any
            - Bond Contract: Covenant, No-Covenant or Any
        3. Discard filters by covenant indicator (keep only `:any`)
        4. Compute Statistics:
            - by SBM, Credit Rating, and SBM by Credit Rating
            - All trades Statistics :
                1. bond count
                2. trade count
                3. trade volume
                4. median trade volume
            - Small Trades (< 100k):
                1. small trades trade count
                2. share of total trade count
                3. small trades trade volume
                4. share of total trade volume
    2. Non-MTN US Corp Bonds

# %%
module MergeDB
    using Distributed

    main_path = "/home/artur/BondPricing/bond-data"
    modules_path = string(main_path, "/modules")
    script_path = string(main_path, "/data-scripts")
    include(string(joinpath(modules_path, "data_module"), ".jl"))
    include(string(joinpath(modules_path, "stats_module"), ".jl"))

    yr = 2019
    qtr = 3
    ARGS=[string(yr), string(qtr)]
    @time include(string(script_path, "/merge_trace_mergent.jl"))

    # Year/Quarter Indicator
    yr_qtr_ind = .&(fdf[:, :trd_exctn_yr] .== yr,
                    fdf[:, :trd_exctn_qtr] .== qtr)

    # Select Variables
    cols = [:cusip_id, :entrd_vol_qt, :all_sample, :ig, :covenant, :ats]

    # Trading System-, Covenant- and Credit-Rating-Contingent Stats

    # All US Corp Bonds ========================================================
    acbdf = fdf[.&(yr_qtr_ind, fdf[:, :all_sample]), cols]

    # ========= Statistics ============
    # Array of Group-By Variables
    gbvars_vec = [Array{Symbol,1}([ ]),
                  [:ats],                 # Stats by SBM
                  [:ig],                  # Stats by CREDIT RATING
                  [:ats, :ig]]            # Stats by SBM, CREDIT RATING

    sdf, atdf, stdf = StatsMod.gen_trade_stats_tables(MergeDB.acbdf, gbvars_vec)
    # ==========================================================================

    # All Non-MTN US Corp Bonds for which covenant info is available ===========
    date_cols = [:trd_exctn_yr, :trd_exctn_qtr]
    cibdf = StatsMod.filter_selected(fdf[yr_qtr_ind, :]; date_cols=date_cols)

    # ========= Statistics ============
    # Array of Group-By Variables
    gbvars_vec = [Array{Symbol,1}([ ]),
                  [:ats],                 # Stats by SBM
                  [:ig],                  # Stats by CREDIT RATING
                  [:covenant],            # Stats by COVENANT
                  [:ats, :covenant],      # Stats by SBM, COVENANT
                  [:ats, :ig],            # Stats by SBM, CREDIT RATING
                  [:ig, :covenant],       # Stats by CREDIT RATING, COVENANT
                  [:ats, :ig, :covenant]]

    csdf, catdf, cstdf = StatsMod.gen_trade_stats_tables(cibdf, gbvars_vec)
    # ==========================================================================
end

# %%
println("Selected bonds (% of total sample): ", (count(MergeDB.fdf[!, :all_sample])/size(MergeDB.fdf, 1))*100)
# %% ===========================================================================

# ==============================================================================
# %% markdown
## Trading System-, Covenant- and Credit-Rating-Contingent Stats

# ==============================================================================
## %% markdown
### Sample of All US Corporate Bonds (with and without Covenants)

# %% All Trades Stats - markdown
println("Statistics for All US Corp Bonds (with and without Covenants)")
println(" ")
StatsMod.markdown_tables_printer(MergeDB.atdf)

# %% All Trades Stats - LaTeX
# f1(x) = typeof(x) == Float64 ? @printf("%.2f", x) : @printf("%s", x)
println("Statistics for All US Corp Bonds (with and without Covenants)")
latexify(convert(Matrix, MergeDB.atdf) ; fmt="%'.2f") |> print

# %% Small Trades Stats - markdown
println("Small Trades Statistics for All US Corp Bonds (with and without Covenants) ")
println(" ")
for sbm in ["ats", "otc", "all"]
    println("Secondary Bond Market: ", uppercase(sbm))
    cols = vcat([:cr, :variable], [Symbol(x) for x in names(MergeDB.stdf)
                                    if occursin(string("_", sbm), x)])
    StatsMod.markdown_tables_printer(MergeDB.stdf[:, cols])
    println(" ")
end

# %% Small Trades Stats - LaTeX
println("Small Trades Statistics for All US Corp Bonds (with and without Covenants) ")
println(" ")
latexify(convert(Matrix, MergeDB.stdf) ; fmt="%'.2f") |> print #display

# %%
println("Secondary Bond Market - Small Trades ")
println(" ")
println("Trade Count ==============================================")
latexify(convert(Matrix, MergeDB.stdf[MergeDB.stdf[:, :variable] .== "trade_count", 2:end]); fmt="%'.2f") |> print
println("==========================================================")
println(" ")
println("Trade Volume =============================================")
latexify(convert(Matrix, MergeDB.stdf[MergeDB.stdf[:, :variable] .== "trade_volume", 2:end]); fmt="%'.2f") |> print
println("==========================================================")
# %% ===========================================================================

# ==============================================================================
# %% markdown
### Sample of All Non-MTN US Corporate Bonds (with and without Covenants)

# %% All Trades Stats - markdown
println("Statistics for All Non-MTN US Corp Bonds (with and without Covenants)")
println(" ")
StatsMod.markdown_tables_printer(MergeDB.catdf)

# %% All Trades Stats - LaTeX
# f1(x) = typeof(x) == Float64 ? @printf("%.2f", x) : @printf("%s", x)
println("Statistics for All Non-MTN US Corp Bonds (with and without Covenants)")
latexify(convert(Matrix, MergeDB.catdf) ; fmt="%'.2f") |> print #display

# %% Small Trades Stats - markdown
println("Small Trades Statistics for All Non-MTN US Corp Bonds (with and without Covenants)")
println(" ")
for sbm in ["ats", "otc", "all"]
    println("Secondary Bond Market: ", uppercase(sbm))
    cols = vcat([:cr, :variable], [Symbol(x) for x in names(MergeDB.cstdf)
                                    if occursin(string("_", sbm), x)])
    StatsMod.markdown_tables_printer(MergeDB.cstdf[:, cols])
    println(" ")
end

# %% Small Trades Stats - LaTeX
println("Small Trades Statistics for All US Corp Bonds (with and without Covenants) ")
println(" ")
# latexify(convert(Matrix, cstdf) ; fmt="%'.2f") |> display

println("Secondary Bond Market - Small Trades ")
println(" ")
println("Trade Count ==============================================")
latexify(convert(Matrix, MergeDB.cstdf[MergeDB.cstdf[:, :variable] .== "trade_count", 2:end]); fmt="%'.2f") |> print
println("==========================================================")
println(" ")
println("Trade Volume =============================================")
latexify(convert(Matrix, MergeDB.cstdf[MergeDB.cstdf[:, :variable] .== "trade_volume", 2:end]); fmt="%'.2f") |> print
println("==========================================================")
# %% ===========================================================================

# %% markdown
## Computing Plot Inputs

# %%
if nprocs() == 1
    addprocs(11)
end
@everywhere main_path = "/home/artur/BondPricing/bond-data"
@everywhere modules_path = string(main_path, "/modules")
@everywhere include(string(joinpath(modules_path, "stats_module"), ".jl"))

save_data=true
small_trd_thrsd = 1e5

# STATS BY COVENANT CATEGORIES ===============================================
scc = @time StatsMod.get_stats_by_cov_cat_df(MergeDB.dto, MergeDB.cibdf;
                                             save_data=save_data)

# Small Trades
small_scc = @time StatsMod.get_stats_by_cov_cat_df(MergeDB.dto, MergeDB.cibdf;
                                                small_trades=true,
                                                small_trd_thrsd=small_trd_thrsd,
                                                save_data=save_data)
# ==============================================================================

# STATS BY NUMBER OF COVENANTS =================================================
snc = @time StatsMod.get_stats_by_num_cov_df(MergeDB.dto, MergeDB.cibdf;
                                             save_data=save_data)

small_snc = @time StatsMod.get_stats_by_num_cov_df(MergeDB.dto, MergeDB.cibdf;
                                                small_trades=true,
                                                small_trd_thrsd=small_trd_thrsd,
                                                save_data=save_data)
# ==============================================================================
println("Finishing computing Plot Input dataframes!")
# %% ===========================================================================

# %% markdown
## Plot Stats Section

# %% Inputs
dto = DataMod.data_obj_constructor()
yr = 2019
qtr = 3

# %% Load the Data
scc = StatsMod.load_stats_data(dto, yr, qtr; stats_by_num_cov=false)
snc = StatsMod.load_stats_data(dto, yr, qtr; stats_by_num_cov=true)

# ADD LOAD OF SMALL TRADES DATAFRAMES
first(scc, 5)


# %%
function get_weighted_stats(df::DataFrame, weight_var::Symbol, sbm::Symbol, rt::Symbol)
        cond = .&(df[:, :sbm] .== sbm, df[:, :rt] .== rt)
        var = occursin("vol", string(weight_var)) ? :total_vol_by_num_cov : :trades_by_num_cov
        variable = occursin("vol", string(weight_var)) ? :trd_volume : :trd_count

        weighted_mean_fun(x, w) = sum(x .* w)/sum(w)
        weighted_std_fun(x, w) = (sum(w .* (x .- weighted_mean_fun(x, w)).^2)/(((size(x, 1) - 1)/size(x, 1)) * sum(w))).^(.5)

        # Number of Covenants
        x = df[cond, :sum_num_cov]

        # Weights
        w = df[cond, var]./sum(df[cond, var])

        # Weighted Mean
        weighted_mean_num_cov = weighted_mean_fun(x, w)

        # Weighted Standard Deviation
        weighted_std_num_cov = weighted_std_fun(x, w)

        return DataFrame(Dict(:sbm => sbm, :rt => rt,
                              :var => variable,
                              :weighted_mean => weighted_mean_num_cov,
                              :weighted_std => weighted_std_num_cov))
end

function get_weighted_stats_table(df::DataFrame;
                                  weight_vars::Array{Symbol, 1}=[:trades_by_num_cov, :total_vol_by_num_cov],
                                  sbm_vec::Array{Symbol, 1}=[:any, :ats, :otc],
                                  rt_vec::Array{Symbol, 1}=[:any, :ig, :hy])

        res_vec = fetch(@spawn [get_weighted_stats(df, w, sbm, rt)
                                for w in weight_vars, sbm in sbm_vec, rt in rt_vec])

        cols = [:var, :sbm, :rt, :weighted_mean, :weighted_std]
        return sort!(vcat(res_vec...)[:, cols], [:var, :sbm, :rt])
end

# %%
res = get_weighted_stats_table(snc)

cond = .&(res[:, :sbm] .!= :any, res[:, :rt] .!= :any)
stdf = stack(res[cond, :], Not([:var, :sbm, :rt]))
df2 = unstack(stdf, :sbm, :value)
df2[:, :diff] = df2[:, :ats] .- df2[:, :otc]
df2

# %% Compute the share of bonds/trades/total volume featuring
# from 5 to 8 distinct covenant categories:
sbm_vec=unique(snc[:, :sbm])
rt_vec=unique(snc[:, :rt])
min_num_cov = 5
max_num_cov = 8
var_vec = [:bonds, :trades, :total_vol] # Excluding issuers because of double-counting
df_vec = fetch(@spawn [StatsMod.num_cov_get_shares(snc, var; sbm_vec=sbm_vec, rt_vec=rt_vec,
                                          min_num_cov=min_num_cov, max_num_cov=max_num_cov)
                       for var in var_vec])
df = innerjoin(df_vec..., on = [:sbm, :rt])
df = df[:, vcat([:sbm, :rt], [Symbol(x, :_share) for x in var_vec])]

# %% Make sure the trade volume histograms sum up to 100%


# %% markdown
## Plots Section

# %% markdown
### Stats By Covenant Categories

# %%
scripts_path = string(main_path, "/data-scripts/plots")
include(string(joinpath(scripts_path, "plot_cov_cat"), ".jl"))
pl

# %% markdown
### Stats by Number of Covenant Categories

# %%
include(string(joinpath(scripts_path, "plot_num_covs"), ".jl"))
pl

# %% ===========================================================================
# == TESTING AREA ==============================================================
# == TESTING AREA ==============================================================
# %%
include(string(joinpath(modules_path, "plot_module"), ".jl"))

dto = DataMod.data_obj_constructor()
run_secs = Dict{Symbol, Bool}(:issuers => false,
                              :bonds => false,
                              :trade_count => true,
                              :trade_volume => true)
run_all = true
[run_secs[k] = true for k in keys(run_secs) if run_all]

# %% Stats by Covenant Categories  =============================================
# Common Parameters
color_scale="viridis"
cal_formula=""
cal_var=""
col_var="cov_cat"
col_var_type="ordinal"
col_title="Covenant Category"
col_sort="ascending"
x_var="sbm:n"
x_var_type="nominal"
x_axis_title=" "
width_step=18
legend_title="Secondary Market"
spacing=4.5
height=350
save_plt=true
plt_type = "cov_cat"
file_ext="png"

pl = []

## Trade Count
stats_var = :count

### SMALL TRADES - Trade Count
small_trades=true
tt = PlotMod.prepare_cat_plot(small_scc; stat=stats_var)
rt_tt = PlotMod.get_ats_otc_diffs_by_rt(small_scc, stats_var)

color_scale="viridis"
cal_formula=""
x_var="sbm:n"
x_var_type="nominal"
x_axis_title=" "
legend_title="Secondary Market"

# y_var="value"
# y_axis_title="Number of Trades"
# title=[string("Small Trades Count of Non-MTN Bonds by Covenant Category")]
# if :period in Symbol.(names(tt))
#     title[end] = string(title[end], " - ", tt[1, :period])
# end
#
# include(string(scripts_path, "/", "single_vega_plt_script.jl"))

y_var="perc_sbm_total"
y_axis_title="% of Total Small Trades Count"
title=["Non-MTN-Bond Small Trades by Covenant Category as a Percentage of the ",
       string("Total Non-MTN-Bond Small Trades Count ",
              "by Secondary Bond Market")]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)

# Trade Count Percentage Diff by Secondary Market {{{2
tt = deepcopy(rt_tt[rt_tt[:, :sbm] .== :otc, :])

color_scale="bluepurple"

cal_formula = ""
cal_legend="Secondary Bond Market"
cal_var=:sbm
x_var="rt:n"
x_var_type="nominal"
legend_title="Rating"
#height=250

y_var="perc_diff"
y_axis_title="% Difference in Small Trades Count"
title=["ATS v.s. OTC % Difference in Rating-Contingent Small Trades " ,
       "Count of Non-MTN-Bonds by Covenant Category"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)

### Done with Small Trades
small_trades=false
# ==============================================================================

# %% Stats by Number of Covenant Categories ====================================
# Common Parameters {{{1
row_var="sum_num_cov"
row_var_type="ordinal"
row_var_title="Number of Covenant Categories per Bond"

col_var="sum_num_cov"
col_var_type="ordinal"
col_sort="ascending"
col_title="Number of Covenant Categories per Bond"

x_var="sbm:n"
x_var_type="nominal"
x_axis_title=" "
width_step=18
legend_title="Secondary Market"
spacing=4.5
height=350
save_plt=true
plt_type = "num_cov"
file_ext="png"

pl = []

## Trade Count
stats_var = :count

### SMALL TRADES - Trade Count
small_trades=true
df = PlotMod.prepare_num_cov_plot(small_snc; stat=stats_var)

# Percentage Trade Count {{{2
color_scale="viridis"

cond = .&(df[:, :sbm] .!= :any, df[:, :rt] .== :any)
tt = df[cond, :]

x_var="sbm:n"
x_var_type="nominal"
legend_title="Secondary Market"

y_var="perc_sbm_total"
y_axis_title="Share of Small Trades"
title=["Secondary-Market-Contingent Share of Small Trades of Non-MTN-Bonds",
       " by Number of Covenant Categories per Bond"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)

### Trade Count Percentage Diff by Secondary Market {{{2
color_scale="bluepurple"

cond = .&(df[:, :sbm] .== :ats, df[:, :rt] .!= :any)
ats_vol  = df[cond, :perc_sbm_rt_total]

cond = .&(df[:, :sbm] .== :otc, df[:, :rt] .!= :any)
otc_vol = df[cond, :perc_sbm_rt_total]
tt  = df[cond, :]
tt[!, :diff] = ats_vol - otc_vol

cal_formula = ""
cal_legend="Secondary Bond Market"
cal_var=:sbm
x_var="rt:n"
x_var_type="nominal"
legend_title="Rating"
#height=250

y_var="diff"
y_axis_title="% Diff in the Number of Small Trades"
title=["ATS v.s. OTC % Difference in Rating- Contingent Number of" ,
       "Non-MTN-Bond Small Trades by Number of Covenant Categories per Bond"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))

# %%
push!(pl, p)

## Done with Small Trades
small_trades=false
# ==============================================================================
