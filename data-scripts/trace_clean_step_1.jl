using DataFrames
using CSV

main_path = "/home/artur/BondPricing/bond-data"
module_path = string(main_path, "/module")
include(string(joinpath(module_path, "data_module"), ".jl"))

# Capture Job Number
job_num = parse(Int, ARGS[1])

dto = DataMod.data_obj_constructor()

# Get List of TRACE Quarterly Files
fl = [x for x in readdir(string(dto.tr.trace_path, "/raw"))]

# Identify file
fn = fl[job_num]

# Extract Year and Quarter
fnsplit = split(split(fn, ".")[1], "_")
file_yr = string(fnsplit[end-1])
file_qtr = string(fnsplit[end])

# Pre-Process TRACE Files
DataMod.trace_preprocesser(dto, file_yr, file_qtr; save_files=true, return_dfs=false)


