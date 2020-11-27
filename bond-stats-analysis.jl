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
## Merge TRACE and MERGENT FISD
1. Merge datasets:
    - Merge on CUSIP
    - For each trade execution date, find the last Rating date
    - Drop is no rating date
2. Create Statistics Variables
    - IG indicator
    - Age and TTM
    - Trade Size Categories
    - ATS indicator (adjust)
3. After the merging process:
    - `:all_sample` indicator: USD Corp Bonds
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
    - Keep only the selected securities (?)
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
println("Statistics for All US Corp Bonds (with and without Covenants) ")
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
@everywhere modules_path = string(main_path, "modules")
@everywhere include(string(joinpath(modules_path, "stats_module"), ".jl"))

save_data=true

# STATS BY COVENANT CATEGORIES
scc = @time StatsMod.get_stats_by_cov_cat_df(MergeDB.dto, MergeDB.cibdf;
                                             save_data=save_data)

# STATS BY NUMBER OF COVENANTS
snc = @time StatsMod.get_stats_by_num_cov_df(MergeDB.dto, MergeDB.cibdf;
                                             save_data=save_data)

println("Finishing computing Plot Input dataframes!")
# %% ===========================================================================

# %% markdown
## Plot Stats Section

# %% Inputs
dto = DataMod.data_obj_constructor()
yr = 2019
qtr = 3

# %% Load the Data
snc = StatsMod.load_stats_data(dto, yr, qtr; stats_by_num_cov=true)
scc = StatsMod.load_stats_data(dto, yr, qtr; stats_by_num_cov=false)
first(scc, 5)

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
