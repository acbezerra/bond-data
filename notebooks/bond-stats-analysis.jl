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
module StatsCal
    using Distributed

    main_path = "/home/artur/BondPricing/bond-data"
    module_path = string(main_path, "/module")
    script_path = string(main_path, "/data-scripts")
    include(string(joinpath(module_path, "data_module"), ".jl"))
    include(string(joinpath(module_path, "stats_module"), ".jl"))

#     yr = 2019
#     qtr = 2
    job_num=14
    ARGS=[string(job_num)]
    @time include(string(joinpath(script_path, "stats_calculator"), ".jl"))
end


# %% markdown
## Plot Stats Section


#  %% Load the Data
dto = DataMod.data_obj_constructor()
yr = 2019
qtr = 3

snc = StatsMod.load_stats_data(dto, yr, qtr; stats_by_num_cov=true)
scc = StatsMod.load_stats_data(dto, yr, qtr; stats_by_num_cov=false)
first(scc, 5)

# %%
main_path = "/home/artur/BondPricing/bond-data"
scripts_path = string(main_path, "/data-scripts/plots")
plt_dir = "plots"
include(string(joinpath(module_path, "plot_module"), ".jl"))

# Common Parameters {{{1
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
# }}}1

stats_var=:issuers
tt = PlotMod.prepare_cat_plot(scc; stat=stats_var)
rt_tt = PlotMod.get_ats_otc_diffs_by_rt(scc, stats_var)
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
y_axis_title="% Difference in the Number of Issuers"
title=["ATS v.s. OTC % Difference in Rating- Contingent Number of Issuers of" ,
       "Non-MTN-Bonds by Covenant Categories"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))


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
first(scc, 3)

# %%
scripts_path = string(main_path, "/data-scripts/plots")
include(string(joinpath(scripts_path, "plot_cov_cat"), ".jl"))
pl

# %%
stats_var = :volume
tt = PlotMod.prepare_cat_plot(scc; stat=stats_var)
rt_tt = PlotMod.get_ats_otc_diffs_by_rt(scc, stats_var)

first(rt_tt, 3)

first(tt, 3)

# %% markdown
### Stats by Number of Covenant Categories

# %%
include(string(joinpath(scripts_path, "plot_num_covs"), ".jl"))
pl
