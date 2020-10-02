using DataFrames
using CSV
using Distributed

main_path = "/home/artur/BondPricing/bond-data"
module_path = string(main_path, "/module")
include(string(joinpath(module_path, "data_module"), ".jl"))

# Capture Job Number
job_num = parse(Int, ARGS[1])

# Define Year and Quarter
min_yr = 2013
max_yr = 2019
yrqtr = DataFrames.crossjoin(DataFrame(:year => min_yr:max_yr), DataFrame(:qtr => 1:4))
yr = yrqtr[job_num, :year]
qtr = yrqtr[job_num, :qtr]

# STEP 3
# Finish Processing Quarterly Files
dto = DataMod.data_obj_constructor()
DataMod.process_trace(dto, yr, qtr; save_proc_df=true, return_proc_df=false)
