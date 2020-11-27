using Printf
using CSV
using Dates
using DataFrames
using DayCounts
# using Revise
# using VegaLite, VegaDatasets

# using Distributed

main_path = "/home/artur/BondPricing/bond-data"
modules_path = string(main_path, "/modules")
script_path = string(main_path, "/data-scripts")
include(string(joinpath(modules_path, "data_module"), ".jl"))
include(string(joinpath(modules_path, "stats_module"), ".jl"))
include(string(joinpath(modules_path, "plot_module"), ".jl"))

# module StatsCal
#     using DataFrames
#     using Distributed
#
#     main_path = "/home/artur/BondPricing/bond-data"
#     modules_path = string(main_path, "modules")
#     script_path = string(main_path, "/data-scripts")
#     include(string(joinpath(modules_path, "data_module"), ".jl"))
#     include(string(joinpath(modules_path, "stats_module"), ".jl"))
#
#     # Define Year and Quarter
#     min_yr = 2016
#     max_yr = 2019
#     yrqtr = DataFrames.crossjoin(DataFrame(:year => min_yr:max_yr),
#                                  DataFrame(:qtr => 1:4))
#     yrqtr[:, :job_num] = 1:size(yrqtr, 1)
#
#     yr = 2019
#     qtr = 3
#     job_num = yrqtr[.&(yrqtr[:, :year] .== yr, yrqtr[:, :qtr] .== qtr), :job_num][1]
#     ARGS=[string(job_num)]
#     @time include(string(joinpath(script_path, "stats_calculator"), ".jl"))
# end

# Capture Job Number
job_num = parse(Int, ARGS[1])
println("job_num: ", job_num)

# Define Year and Quarter
min_yr = 2016
max_yr = 2019
yrqtr = DataFrames.crossjoin(DataFrame(:year => min_yr:max_yr),
                             DataFrame(:qtr => 1:4))

# TRACE
yr = yrqtr[job_num, :year]
qtr = yrqtr[job_num, :qtr]


# Merge TRACE and MERGENT
empty!(ARGS)
push!(ARGS, string(yr), string(qtr))
@time include(string(script_path, "/merge_trace_mergent.jl"))


dto = DataMod.data_obj_constructor()

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
