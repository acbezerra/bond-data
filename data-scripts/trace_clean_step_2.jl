using DataFrames
using CSV
using Distributed

main_path = "/home/artur/BondPricing/bond-data"
module_path = string(main_path, "/module")
include(string(joinpath(module_path, "data_module"), ".jl"))

min_yr = 2013
max_yr = 2019

dto = DataMod.data_obj_constructor()

# STEP 2
# Collect cancel and reverse quarterly files into yearly files.

# Collect Quarterly Pre-Processed Files
for yr in min_yr:max_yr
    @time DataMod.group_pre_proc_by_yr(dto, yr; df_type="cancel")
    @time DataMod.group_pre_proc_by_yr(dto, yr; df_type="reverse")
end

