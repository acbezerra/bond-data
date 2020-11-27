using DataFrames
using Printf
using CSV
using Dates
using Revise
using Distributed

main_path = "/home/artur/BondPricing/bond-data"
modules_path = string(main_path, "/modules")
script_path = string(main_path, "/data-scripts")
include(string(joinpath(modules_path, "data_module"), ".jl"))

# * Inputs
# Mergent
rmnp=true
min_maturity = 20111231
filter_rating = true
rating_types = ["MR", "SPR"]
bond_id = :ISSUE_ID
eval_merge=true

# TRACE
yr = 2019
qtr = 1

# Dates
miss_date = 11111111

# * Load Objects and Data
tro = DataMod.trace_obj_constructor()

# ** Mergent Bond Issue + Rating Data
# this will filter the mergent data to keep
# only the bonds with maturity > min_maturity
# and rating data from a rating agency in rating_types.
mdf = @time DataMod.get_mergent_fisd_df(tro;
                                         rmnp=rmnp,
                                         min_maturity=min_maturity,
                                         filter_rating=filter_rating,
                                         rating_types=rating_types,
                                         bond_id=bond_id,
                                         eval_merge=eval_merge,
                                         miss_date=miss_date)


# ** Load Trace Filtered DataFrame
filter_fpath = string(tro.trace_path, "/", tro.filter_dir)
filter_fname = string(tro.filter_file_prefix, "_", yr, "_Q", qtr, ".csv")
tdf = @time DataFrame!(CSV.File(string(filter_fpath, "/", filter_fname), types=tro.colsd))

# * Merge TRACE and MERGENT DataFrames, run IG classifier
fdf = @time DataMod.merge_trace_mergent_dfs(tro, tdf, mdf)
