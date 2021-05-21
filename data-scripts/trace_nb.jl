# %%
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

# %% Inputs
# Mergent
rmnp=true
min_maturity = 20111231
filter_rating = true
rating_types = ["MR", "SPR"]
bond_id = :ISSUE_ID
eval_merge=true

# TRACE
yr = 2019
qtr = 3

# Dates
miss_date = 11111111

# %% Load Objects and Data
tro = DataMod.data_obj_constructor()

# %% Mergent Bond Issue + Rating Data
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

# %% Load Trace Filtered DataFrame
filter_fpath = string(tro.tr.trace_path, "/", tro.tr.filter_dir)
filter_fname = string(tro.tr.filter_file_prefix, "_", yr, "_Q", qtr, ".csv")
tdf = @time DataFrame!(CSV.File(string(filter_fpath, "/", filter_fname), types=tro.tr.colsd))

# %% Merge TRACE and MERGENT DataFrames, run IG classifier
fdf, mdiag = @time DataMod.merge_trace_mergent_dfs(tro, tdf, mdf)


# %%
tr_cusips = unique(tdf[:, :cusip_id])
mf_cusips = unique(mdf[:, :COMPLETE_CUSIP])
println(string("Unique CUSIPS in trace dataframe: ", size(tr_cusips, 1)))
println(string("Unique CUSIPS in FISD dataframe: ", size(mf_cusips, 1)))

# %% Unmatched CUSIPs
mtc = intersect(tr_cusips, mf_cusips)
umtc = setdiff(tr_cusips, mtc)

# %%
size(mtc, 1)/size(tr_cusips,1)
size(umtc, 1)/size(tr_cusips,1)

# %% List characteristics of the unmatched bonds
cols = [:cusip_id, :bond_sym_id, :company_symbol, :scrty_type_cd, :bloomberg_identifier, :sub_prdct]
idx_cusips = findall(in(umtc), tdf[:, :cusip_id])
tmp = unique(tdf[idx_cusips, cols])

# %%
first(tmp, 20)

# %%
unique(tmp[:, :sub_prdct])
