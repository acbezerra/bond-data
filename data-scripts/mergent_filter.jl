# vim: set fdm=marker :

using DataFrames
using CSV
using Dates
using Distributed

main_path = "/home/artur/BondPricing/bond-data"
module_path = string(main_path, "/module")
script_path = string(main_path, "/data-scripts")
include(string(joinpath(module_path, "data_module"), ".jl"))

# * Inputs
# Mergent
rmnp=true
min_maturity = 20111231
filter_rating = true
rating_types = ["MR", "SPR"]
bond_id = :ISSUE_ID
eval_merge=true
# Dates
miss_date = 11111111

# Load Objects and Data
dto = DataMod.data_obj_constructor()

# MERGE TRACE AND MERGENT FISD {{{1
# Mergent Bond Issue + Rating Data
# this will filter the mergent data to keep
# only the bonds with maturity > min_maturity
# and rating data from a rating agency in rating_types.
mdf = @time DataMod.get_mergent_fisd_df(dto;
                                        rmnp=rmnp,
                                        min_maturity=min_maturity,
                                        filter_rating=filter_rating,
                                        rating_types=rating_types,
                                        bond_id=bond_id,
                                        eval_merge=eval_merge,
                                        miss_date=miss_date)

# %% Corporate Bond Type Indicator {{{2
cpf(x) = Symbol(x) in keys(DataMod.corp_bond_types)
mdf[!, :corp_bond] .= cpf.(mdf[:, :BOND_TYPE])

# USD indicator
# mdf[!, :usd] .= .&(mdf[!, :CURRENCY] .!== missing, mdf[!, :CURRENCY] .== "USD")
mdf[!, :usd] = .&(mdf[!, :FOREIGN_CURRENCY] .!== missing, mdf[!, :FOREIGN_CURRENCY] .== "N")

# MTN indicator
mdf[!, :mtn] = .&(mdf[!, :MTN] .!== missing, mdf[!, :MTN] .== "Y")

# Subsequent Data indicator
mdf[!, :subseq_data] = .&(mdf[:, :SUBSEQUENT_DATA] .!== missing, mdf[:, :SUBSEQUENT_DATA] .== "Y")

# US Corporate Bonds with no covenant data
mdf[!, :cov_data] = .|([(mdf[!, col] .!== missing) for
                        col in keys(DataMod.vars2covgr)]...)

# Selected Bonds: Dollar-Denominated US Corporate Bonds,
# excluding non-MTN bonds with missing covenant AND subsequent data
non_mtn_selected = .&(mdf[!, :usd], mdf[!, :corp_bond], .!mdf[!, :mtn],
                      .&(.!mdf[!, :cov_data], .!mdf[!, :subseq_data]) .== false)
# excluding MTNs without any cov information and 
mtn_selected = .&(mdf[!, :usd], mdf[!, :corp_bond], 
                  mdf[:, :mtn], mdf[!, :cov_data])

mdf[!, :selected] = non_mtn_selected .| mtn_selected
# }}}
# Convertible Bond Indicator {{{2
conv_cols = [x for x  in vcat(DataMod.convertible_cols, DataMod.convertible_add_cols) if
              !(x in [:ISSUE_ID])]
mdf[!, :convertible] = .|([.&(mdf[:, col] .!== missing, mdf[:, col] .!= "N") for col in conv_cols]...)

# }}}

# %% Dollar-Denominated US Corporate Bonds Analysis
include(string(joinpath(script_path, "mergent_analysis"), ".jl"))

# Create Covenant Groups
mdf = DataMod.covgr_indicators(mdf)

# Covenant Indicator
mdf[!, :covenant] = .|([mdf[:, col] for col in names(mdf) if occursin("cg", col) ]...)

# Drop columns
# keep_cols = [x for x in Symbol.(names(mdf)) if (x in DataMod.mergent_vars_keep)]
# mdf = mdf[:, keep_cols]

# mdf[!, :CONVERTIBLE_ORIG] .= mdf[!, :CONVERTIBLE]

# # Convert to Booleans:
# bool_cols = [x for x in Symbol.(names(mdf)) if (x in DataMod.convert2bool_cols) ]
# for col in bool_cols
#     println(string("Converting variable ", col, " to Boolean..."))
#     mdf[!, col] .= DataMod.convert_2_bool(mdf, col)
# end

# Save DataFrame
mdf_fpath = dto.mf.mergent_path
mdf_fname =string("mergent_", dto.mf.filter_file_prefix, ".csv")

println(string("Saving mergent_",  dto.mf.filter_file_prefix, " dataframe..."))
CSV.write(string(mdf_fpath, "/", mdf_fname), mdf)
