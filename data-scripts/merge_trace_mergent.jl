# vim: set fdm=marker :

using DataFrames
using Printf
using CSV
using Dates
using DayCounts
using Revise
using Distributed

main_path = "/home/artur/BondPricing/bond-data"
module_path = string(main_path, "/module")
script_path = string(main_path, "/data-scripts")
include(string(joinpath(module_path, "data_module"), ".jl"))

# Capture Job Number
# job_num = parse(Int, ARGS[1])
# job_num=14      # 2019Q2

# # Define Year and Quarter
# min_yr = 2016
# max_yr = 2019
# yrqtr = DataFrames.crossjoin(DataFrame(:year => min_yr:max_yr), DataFrame(:qtr => 1:4))

# TRACE
# yr = yrqtr[job_num, :year]
# qtr = yrqtr[job_num, :qtr]

yr = parse(Int, ARGS[1])
qtr = parse(Int, ARGS[2])
println("TRACE year: ", yr)
println("TRACE quarter: ", qtr)

# MERGED
save_df=true

# Load Objects and Data
dto = DataMod.data_obj_constructor()

# MERGE TRACE AND MERGENT FISD {{{1
# ## Step 1 - Get MERGENT Filtered Dataset #####################################
# Making re-computing optional.
load_mdf = true

# Check if filtered MERGENT dataframe exists
dto = DataMod.data_obj_constructor()
mdf_fpath = dto.mf.mergent_path
mdf_fname =string("mergent_", dto.mf.filter_file_prefix, ".csv")
# Update condition accordingly
load_mdf = .&(load_mdf, isfile(string(mdf_fpath, "/", mdf_fname)))

# Get the dataframe
mdf = DataFrame()
if load_mdf
    # load filtered dataframe
    mdf = @time DataMod.load_mergent_filtered_df(dto; drop_cols=true)
else
    # load original files, merge and filter:
    include(string(script_path, "/mergent_filter.jl"))
end
# ################################################################################

# ## Step 2 - Get TRACE Filtered Dataset #########################################
# Load Trace Filtered DataFrame
filter_fpath = string(dto.tr.trace_path, "/", dto.tr.filter_dir)
filter_fname = string(dto.tr.filter_file_prefix, "_", yr, "_Q", qtr, ".csv")
tdf = @time DataFrame!(CSV.File(string(filter_fpath, "/", filter_fname), 
                                types=dto.tr.colsd))

# Drop columns
trcols = [x for x in Symbol.(names(tdf)) if x in 
          vcat(keys(DataMod.trace_keep_dict)..., keys(DataMod.trace_ud_keep_dict)...)]
tdf = tdf[!, trcols]
# #################################################################################

# Merge TRACE and MERGENT DataFrames, run IG classifier 
fdf, mdiag = @time DataMod.merge_trace_mergent_dfs(dto, tdf, mdf; mcols=[])
# }}}
# CREATE VARIABLES {{{1
# IG Indicator, AGE and TTM, Trade Size Categories and ATS Indicator
fdf = DataMod.create_stats_vars(fdf)
# }}}
# SAVE RESULTS  {{{1
# Path to Processed File
fpath = string(dto.main_path, "/", dto.data_dir, "/", dto.merged_dir)

# File Name
fname = string(dto.merged_file_prefix, "_", yr, "_Q", qtr, ".csv")
diag_fname = string("merge_diag_", yr, "_Q", qtr, ".csv")

# # Save File
# println(string("Saving merged  ", yr, " Q", qtr, " Quarterly Files..."))
# As it turns out, the complete fdf dataframe is too large to be
# saved. Better to save only the stats results
# CSV.write(string(fpath, "/", fname), fdf)

# CSV.write(string(fpath, "/", diag_fname), mdiag)
# }}}
