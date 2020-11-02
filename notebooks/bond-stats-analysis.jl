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
    qtr = 2
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

    job_num=14
    ARGS=[string(job_num)]
    @time include(string(joinpath(script_path, "stats_calculator"), ".jl"))
    #     yr = 2019
    #     qtr = 2
end

# %% markdown
## Plot Stats Section


#  %% Load the Data
dto = DataMod.data_obj_constructor()
yr = 2019
qtr = 2

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
