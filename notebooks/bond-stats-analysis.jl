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
# @everywhere module_path = string(main_path, "/module")
# @everywhere script_path = string(main_path, "/data-scripts")
# @everywhere include(string(joinpath(module_path, "data_module"), ".jl"))
# @everywhere include(string(joinpath(module_path, "stats_module"), ".jl"))
# @everywhere include(string(joinpath(module_path, "plot_module"), ".jl"))

main_path = "/home/artur/BondPricing/bond-data"
module_path = string(main_path, "/module")
script_path = string(main_path, "/data-scripts")
include(string(joinpath(module_path, "data_module"), ".jl"))
include(string(joinpath(module_path, "stats_module"), ".jl"))
include(string(joinpath(module_path, "plot_module"), ".jl"))


ENV["LINES"] = 100
ENV["COLUMNS"] = 1000


# %%
module MergeDB
    using Distributed

    main_path = "/home/artur/BondPricing/bond-data"
    module_path = string(main_path, "/module")
    script_path = string(main_path, "/data-scripts")
    include(string(joinpath(module_path, "data_module"), ".jl"))
    include(string(joinpath(module_path, "stats_module"), ".jl"))

    yr = 2019
    qtr = 3
    ARGS=[string(yr), string(qtr)]
    @time include(string(script_path, "/merge_trace_mergent.jl"))
end

# %%
fdf = deepcopy(MergeDB.fdf)
first(fdf, 5)


# %% markdown
## Trading System-, Covenant- and Credit-Rating-Contingent Stats
Sample of All Non-MTN Bonds (with and without Covenants)

# %%
# @everywhere include(string(joinpath(module_path, "stats_module"), ".jl"))
include(string(joinpath(module_path, "stats_module"), ".jl"))

# Create Trade Execution Quarter:
fdf[:, :trd_exctn_qtr] = ceil.(fdf[:, :trd_exctn_mo] / 3.)

# Year/Quarter Indicator
yr_qtr_ind = .&(fdf[:, :trd_exctn_yr] .== MergeDB.yr,
                fdf[:, :trd_exctn_qtr] .== MergeDB.qtr)

# Select Variables
cols = [:cusip_id, :entrd_vol_qt, :ig, :covenant, :ats]

# Filter DataFrame
ffdf = fdf[yr_qtr_ind, cols]

# Array of Group-By Variables
gbvars_vec = [Array{Symbol,1}([ ]),
              [:ats],                 # Stats by SBM
              [:ig],                  # Stats by CREDIT RATING
              [:covenant],            # Stats by COVENANT
              [:ats, :covenant],      # Stats by SBM, COVENANT
              [:ats, :ig],            # Stats by SBM, CREDIT RATING
              [:ig, :covenant],       # Stats by CREDIT RATING, COVENANT
              [:ats, :ig, :covenant]]


@time sdf_vec = fetch(@spawn [StatsMod.compute_stats(ffdf,gbvars) for gbvars in gbvars_vec])
sdf = vcat(sdf_vec...)

df = StatsMod.form_stats_table(sdf)

# %%
latexify(convert(Matrix, df[:, 2:end]); fmt="%.2f") |> display

# %%
latexify(convert(Matrix, df[:, 2:end]); fmt="%.2f") |> print

# %% markdown
## Computing Plot Inputs

# %%
using Statistics
using Distributed
include(string(joinpath(module_path, "data_module"), ".jl"))
include(string(joinpath(module_path, "stats_module"), ".jl"))

dto = DataMod.data_obj_constructor()

# @time include(string(script_path, "/merge_trace_mergent.jl"))

# Generate Trade Execution Quarter variable:
if !(:trd_exctn_qtr in Symbol.(names(fdf)))
    fdf[!, :trd_exctn_qtr] .= Int64.(ceil.(fdf[!, :trd_exctn_mo]/3))
end

# Groupby Date Variables:
date_cols = [:trd_exctn_yr, :trd_exctn_qtr]
# Form combinations of ATS, IG and COVENANT filters
combdf =  StatsMod.get_filter_combinations()

# STATS BY COVENANT CATEGORIES #######################################
# Select cols and create smk indicator variable:
ffdf = StatsMod.filter_selected(fdf; date_cols=date_cols)

dfl_qtr = @time fetch(@spawn [StatsMod.stats_generator(ffdf,
                                       StatsMod.dfrow2dict(combdf, row);
                                       groupby_date_cols=date_cols)
                          for row in 1:size(combdf, 1)])
scc = sort(vcat(dfl_qtr...), names(combdf))
scc = StatsMod.gen_sbm_rt_cvt_cat_vars(scc)
StatsMod.save_stats_data(dto, scc)

# STATS BY NUMBER OF COVENANTS #######################################
# Keep only the selected securities
fdf = fdf[fdf[:, :selected], :]

fdf[!, :sum_num_cov] .= sum([fdf[:, Symbol(:cg, x)] for x in 1:15])
dfl = []
combdf =  StatsMod.get_filter_combinations()
combdf = StatsMod.gen_sbm_rt_cvt_cat_vars(combdf)

dfl = @time fetch(Distributed.@spawn [StatsMod.compute_stats_by_num_cov(fdf, sbm, rt, combdf) for
                    sbm in [:any, :ats, :otc], rt in [:any, :ig, :hy]])

snc = vcat(dfl...)
StatsMod.save_stats_data(dto, snc)

# %%


# %% markdown
## Plot Stats Section

# %%
module StatsCal
    using Distributed

    main_path = "/home/artur/BondPricing/bond-data"
    module_path = string(main_path, "/module")
    script_path = string(main_path, "/data-scripts")
    include(string(joinpath(module_path, "data_module"), ".jl"))
    include(string(joinpath(module_path, "stats_module"), ".jl"))

#     yr = 2019
#     qtr = 2
    job_num=15
    ARGS=[string(job_num)]
    @time include(string(joinpath(script_path, "stats_calculator"), ".jl"))
end

# %%
StatsCal.snc

# %%
dto = DataMod.data_obj_constructor()

#  %% Load the Data
yr = 2019
qtr = 3

snc = StatsMod.load_stats_data(dto, yr, qtr; stats_by_num_cov=true)
scc = StatsMod.load_stats_data(dto, yr, qtr; stats_by_num_cov=false)
first(scc, 5)

# %%
include(string(joinpath(module_path, "plot_module"), ".jl"))
stats_var = :issuers
rt_tt = PlotMod.get_ats_otc_diffs_by_rt(scc, stats_var)
first(rt_tt, 10)

# %%
x_var="rt:n"
x_var_type="nominal"
legend_title="Rating"

rt_tt[:, "sbm2"] .= uppercase.(string.(rt_tt[:, :sbm]))
row_var="sbm2"
y_var = "perc_sbm_total"
col_var = "cov_cat"
row_var_type="ordinal"
row_var_title="Covenant Categories"
y_axis_title=""
title = ["smth smth"]
save_plt = false

p = PlotMod.dual_vega_plt(rt_tt, col_var, x_var, y_var,  row_var;
                         row_var_type=row_var_type,
                         row_var_title=row_var_title,
                         col_var_type=col_var_type,
                         col_title=col_title,
                         col_sort=col_sort,
                         x_var_type=x_var_type,
                         x_axis_title=x_axis_title,
                         y_axis_title=y_axis_title,
                         legend_title=legend_title,
                         color_scale=color_scale,
                         title=title,
                         spacing=spacing,
                         width_step=width_step,
                         height=height,
                         save_plt=save_plt,
                         plt_type=plt_type,
                         stats_var=stats_var)
#                          main_path=main_path,
#                          opacity=.2,
#                          plt_dir=plt_dir,
#                          file_ext=file_ext)

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
