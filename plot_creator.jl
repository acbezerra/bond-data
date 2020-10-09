# %%
using DataFrames
using Printf
using CSV
using Dates
using DayCounts
# using Revise
using FileIO
using VegaLite

main_path = "/home/artur/BondPricing/bond-data"
module_path = string(main_path, "/module")
script_path = string(main_path, "/data-scripts/plots")
include(string(joinpath(module_path, "data_module"), ".jl"))
include(string(joinpath(module_path, "stats_module"), ".jl"))
include(string(joinpath(module_path, "plot_module"), ".jl"))
ENV["LINES"] = 100
ENV["COLUMNS"] = 1000

# %%
dto = DataMod.data_obj_constructor()

# %%
yr = 2019
qtr = 2
include(string(joinpath(module_path, "stats_module"), ".jl"))
snc = StatsMod.load_stats_data(dto, yr, qtr; stats_by_num_cov=true)
scc = StatsMod.load_stats_data(dto, yr, qtr; stats_by_num_cov=false)

first(scc, 5)

# %%
include(string(joinpath(script_path, "plot_cov_cat"), ".jl"))
pl


# %%
include(string(joinpath(script_path, "plot_num_covs"), ".jl"))
pl
