# vim: set fdm=marker :

module DataMod

using CSV
using DataFrames
using Distributed
using Dates
using DayCounts
using Statistics

# Inputs {{{1
main_path="/home/artur/BondPricing/bond-data"
data_dir="data"

filter_file_prefix="filtered"

# TRACE Data Inputs
include(string(main_path, "/module/trace_inputs.jl"))

# MERGENT FISD Data Inputs
include(string(main_path, "/module/mergent_inputs.jl"))

# Merged Datasets
merged_dir="MERGED"
merged_file_prefix="merged"

# Statistics
stats_dir="Stats"
# }}}
# Data Objects {{{1
mutable struct trace_obj
   trace_dir::String
   trace_path::String
   raw_files_dir::String
   pre_proc_dir::String
   proc_dir::String
   filter_dir::String

   trace_file_prefix::String
   cancel_trd_file_prefix::String
   reverse_trd_file_prefix::String
   original_trd_file_prefix::String
   proc_file_prefix::String
   filter_file_prefix::String

   cancel_df_cols::Array{String,1}
   rev_df_cols::Array{String,1}

   colsd::Dict{String, DataType}
end

mutable struct mergent_obj
   mergent_dir::String
   mergent_path::String
   mergent_file_prefix::String
   filter_file_prefix::String
   colsd::Dict{Symbol,Array{Symbol,1}}
end

mutable struct data_obj
   main_path::String
   data_dir::String
   merged_dir::String
   stats_dir::String
   merged_file_prefix::String
   tr   # trace object
   mf   # mergent fisd object
end
# }}}
# Constructor {{{1
function data_obj_constructor(; main_path::String=main_path,
                               data_dir::String=data_dir,
                               trace_dir::String=trace_dir,
                               mergent_dir::String=mergent_dir,
                               merged_dir::String=merged_dir,
                               raw_files_dir::String=raw_files_dir,
                               pre_proc_dir::String=pre_proc_dir,
                               proc_dir::String=proc_dir,
                               filter_dir::String=filter_dir,
                               stats_dir::String=stats_dir,
                               trace_file_prefix::String=trace_file_prefix,
                               cancel_trd_file_prefix::String=cancel_trd_file_prefix,
                               reverse_trd_file_prefix::String=reverse_trd_file_prefix,
                               original_trd_file_prefix::String=original_trd_file_prefix,
                               proc_file_prefix::String=proc_file_prefix,
                               filter_file_prefix::String=filter_file_prefix,
                               mergent_file_prefix::String=mergent_file_prefix,
                               merged_file_prefix::String=merged_file_prefix,
                               cancel_df_cols::Array{String, 1}=cancel_df_cols,
                               rev_df_cols::Array{String, 1}=rev_df_cols,
                               trace_cols_dict::Dict{String, DataType}=trace_cols_dict,
                               mergent_cols_dict::Dict{Symbol,Array{Symbol,1}}=mergent_cols_dict)

  # Path to Data Files
  trace_path = string(main_path, "/", data_dir, "/", trace_dir)
  mergent_path = string(main_path, "/", data_dir, "/", mergent_dir)

  tr = trace_obj(trace_dir, trace_path, 
                 raw_files_dir, pre_proc_dir,
                 proc_dir, filter_dir,
                 trace_file_prefix, cancel_trd_file_prefix,
                 reverse_trd_file_prefix, original_trd_file_prefix,
                 proc_file_prefix, filter_file_prefix, 
                 cancel_df_cols, rev_df_cols, trace_cols_dict)

  mf = mergent_obj(mergent_dir, 
                   mergent_path,
                   mergent_file_prefix,    
                   filter_file_prefix, 
                   mergent_cols_dict)

  return data_obj(main_path, data_dir, 
                  merged_dir, stats_dir,
                  merged_file_prefix,
                  tr, mf) 
end
# }}}
# TRACE {{{1
# STEP 1: Pre-Processing {{{2
function save_preprocessed_df(dto, df::DataFrame,
                              file_yr::String, file_qtr::String;
                              savedf::Bool=true)

    iterate_years = false
    fpath = string(dto.tr.trace_path, "/", dto.tr.pre_proc_dir)
    file_prefix = string(fpath, "/", dto.tr.original_trd_file_prefix)
    if any(["X" in unique(df[:, "trc_st"]), "C" in unique(df[:, "trc_st"])])
        file_prefix = string(fpath, "/", dto.tr.cancel_trd_file_prefix)
        iterate_years = true
    elseif "Y" in unique(df[:, "trc_st"])
        file_prefix = string(fpath, "/", dto.tr.reverse_trd_file_prefix)
        iterate_years = true
    end

    if iterate_years
        yrs = unique(df[:, :trd_exctn_yr])
        for yr in yrs
            # Filter
            cond = abs.(df[:, :trd_exctn_yr] .- yr) .< 1e-5
            tmp_df = df[cond, :]

            # File Path Name
            tmp_name = string(file_prefix, "_", file_yr, "_", file_qtr, "_", yr, ".csv")

            # Save File
            CSV.write(tmp_name, tmp_df)
        end
    else
        # File Path Name
        df_name = string(file_prefix, "_", file_yr, "_", file_qtr, ".csv")

        # Save File
        CSV.write(df_name, df)
    end
end

function trace_preprocesser(dto, file_yr::String, file_qtr::String;
                            save_files::Bool=true, return_dfs::Bool=false)

    # File Path Name
    fpname = string(dto.tr.trace_path, "/", dto.tr.raw_files_dir, "/", dto.tr.trace_file_prefix,
                    "_", file_yr, "_", file_qtr, ".csv")

    println(fpname)

    df = @time DataFrame!(CSV.File(fpname, types=dto.tr.colsd))

    # Delete rows with missing cusip_id
    df = df[ismissing.(df[:, "cusip_id"]) .== false, :]

    # Extract Year and Months
    df[!,"trd_exctn_yr"] .= parse.(Int64, first.(string.(df[:, "trd_exctn_dt"]), 4))
    df[!,"trd_exctn_mo"] .= parse.(Int64, last.(first.(string.(df[:, "trd_exctn_dt"]), 6), 2))
    df[!, :trd_exctn_qtr] .= Int64.(ceil.(df[!, :trd_exctn_mo]/3))
    yr_mo_vars = ["trd_exctn_yr", "trd_extn_mo"]

    # Cancelled Trades and Cancelled Corrections
    cancel_cond = df[:, "trc_st"] .∈ Ref(["X", "C"])

    # Reversals
    rev_cond = df[:, "trc_st"] .== "Y"

    cancel_trd_df = df[cancel_cond, vcat(yr_mo_vars, dto.tr.cancel_df_cols)]
    reverse_trd_df = df[rev_cond, vcat(yr_mo_vars, dto.rev_df_cols)]

    # Remaining Data
    original_trd_df = df[(cancel_cond .| rev_cond) .== false, :]
    # filter!(row -> !(row["trc_st"] in ["X", "C", "Y"]), df)

    # Save Files
    if save_files
      save_preprocessed_df(dto, cancel_trd_df, file_yr, file_qtr)
      save_preprocessed_df(dto, reverse_trd_df, file_yr, file_qtr)
      save_preprocessed_df(dto, original_trd_df, file_yr, file_qtr)
    end

    if return_dfs
       return cancel_trd_df, reverse_trd_df, original_trd_df
    end
end
# }}}
# STEP 2: Collecting Quarterly Pre-Processed Files {{{2
function group_pre_proc_by_yr(dto, yr::Int64;
                              df_type::String="cancel",
                              save_df::Bool=true, return_df::Bool=false)
    # Read Files in the Pre-Processed Directory
    pre_proc_fpath = string(dto.tr.trace_path, "/", dto.tr.pre_proc_dir)
    files_list = [x for x in readdir(pre_proc_fpath) if occursin(string("_", yr, ".csv"), x)]

    # List files
    if df_type == "cancel"
        # Select only the deleteI files
        fname_id = dto.tr.cancel_trd_file_prefix
    elseif df_type == "reverse"
        # Select only the deleteII files
        fname_id = dto.tr.reverse_trd_file_prefix
     else
        println("Error! Dataframe type not recognized.")
        println("Please enter 'cancel' or 'reverse'.")
        println("Exiting...")
        return
    end
    del_fl = [x for x in files_list if occursin(fname_id, x)]

    # List files from the selected year
    del_yr_files = [fn for fn in del_fl if occursin(string(yr), split(fn, "_")[end])]

    # Define loader function
    df_loader(fl::String) = DataFrame!(CSV.File(string(pre_proc_fpath, "/", fl); types=dto.tr.colsd))

    # list of dataframes
    dfl = fetch(@spawn [df_loader(fl) for fl in del_yr_files])
    df = vcat(dfl...)


    if save_df
        println(string("Saving grouped ", yr, " ", fname_id, " dataframes..."))
        dfname = string(split(del_yr_files[1], string(yr))[1], yr, "_all.csv")
        CSV.write(string(pre_proc_fpath, "/", dfname), df)
    end

    if return_df
        return df
    end
end
# }}}
# STEP 3: Process Quarterly Files {{{2
function process_trace(dto, file_yr::Int64, file_qtr::Int64;
                       save_proc_df::Bool=true, return_proc_df::Bool=false)
    # Path to Pre-Processed Files
    fpath = string(dto.tr.trace_path, "/", dto.tr.pre_proc_dir)

    # Load Pre-Processed Main File (Original Trades)
    println(string("Loading Main Trades ", file_yr, " Q", file_qtr, " Quarterly File..."))
    orig_fname = string(dto.tr.original_trd_file_prefix, "_", file_yr, "_Q", file_qtr, ".csv")
    # Check if file exists
    if !(orig_fname in readdir(fpath))
        println("File not found! Exiting...")
        return
    end
    odf = @time DataFrame!(CSV.File(string(fpath, "/", orig_fname), types=dto.tr.colsd))


    # Cancelled Trades
    println(string("Loading Cancelled Trades ", file_yr, " Yearly File..."))
    cancel_fl = string(dto.tr.cancel_trd_file_prefix, "_", file_yr, "_all.csv")
    cdf = @time DataFrame!(CSV.File(string(fpath, "/", cancel_fl), types=dto.tr.colsd))

    # Reversed Trades
    println(string("Loading Reversed Trades ", file_yr, " Yearly File..."))
    reverse_fl = string(dto.tr.reverse_trd_file_prefix, "_", file_yr, "_all.csv")
    rdf = @time DataFrame!(CSV.File(string(fpath, "/", reverse_fl), types=dto.tr.colsd))

    # Filter out Cancelled Trades
    println("Filtering out cancelled trades...")
    colsI = [x for x in dto.tr.cancel_df_cols if x != "trc_st"]
    odf = @time DataFrames.antijoin(odf, cdf; on=colsI)

    # remove reversals with missing orig_msg_seq_nb
    nmcond = ismissing.(rdf[:, "orig_msg_seq_nb"]) .== false
    rdf = rdf[nmcond, :]

    # Filter out Reversals
    println("Filtering out reversals...")
    colsl = [x => x for x in colsI if x != "msg_seq_nb"]
    push!(colsl, "msg_seq_nb" => "orig_msg_seq_nb")
    mdf = @time DataFrames.antijoin(odf, rdf; on=colsl)

    if save_proc_df
        # Path to Processed File
        fpath = string(dto.tr.trace_path, "/", dto.tr.proc_dir)

        # File Name
        fname = string(dto.tr.proc_file_prefix, "_", file_yr, "_Q", file_qtr, ".csv")

        # Save File
        println(string("Saving processed ", file_yr, " Q", file_qtr, " Quarterly File..."))
        @time CSV.write(string(fpath, "/", fname), odf)
    end

    if return_proc_df
        return odf
    end
end
# }}}
# STEP 4: Filter Agency Trades et al {{{2

# Remove Dealer-Customer Agency Trades without Commission {{{3
function filter_agency_trades(df::DataFrame;
                                              del_nca_trds::Bool=false)
    # Reporting Side Capacity
    df[!, "rpt_side_cpcty"] .= string()

    # Reporting Side: Buyer
    cond = df[:, "rpt_side_cd"] .== "B"
    df[cond, "rpt_side_cpcty"] .= Array{String,1}(df[cond, "buy_cpcty_cd"])

    # Reporting Side: Seller
    cond = df[:, "rpt_side_cd"] .== "S"
    df[cond, "rpt_side_cpcty"] .= Array{String,1}(df[cond, "sell_cpcty_cd"])

    # Agency transactions without commission
    if del_nca_trds
      nccond = .&(df[:, "rpt_side_cpcty"] .== "A", df[:, "cntra_mp_id"] .== "C",
                 .&(df[:, "cmsn_trd"] .== "N", ismissing.(df[:, "cmsn_trd"]) .== false))
      # Remove Dealer-Customer Agency Transactions Without Commission
      return df[.!nccond, :]
    end

    return df
end
# }}}
# Remove double reporting of interdealer transactions {{{3
function remove_interdealer_buyer_side_reports(df::DataFrame;
                                               rename_rpt_party::Bool=true)
    # Deletes interdealer transactions (one of the sides);
    cond  = .&(df[:, :cntra_mp_id] .== "D", df[:, :rpt_side_cd] .== "B")

    # Renames the reporting party side indicator to include a D for interdealer;
    # if cntra_mp_id = ’D’ and rpt_side_cd=’S’ then rpt_side_cd=’D’;
    if rename_rpt_party
      println(string("Renaming the reporting party side indicator from 'S' to 'D' in ",
                     "interdealer trades..."))
      id_cond = .&(df[:, :cntra_mp_id] .== "D", df[:, :rpt_side_cd] .== "S")
      df[id_cond, :rpt_side_cd] .= "D"
    end

   return df[.!cond, :]
end
# }}}
# Filter Special Trades {{{3
function filter_special_conditions(df::DataFrame;
                                   del_wi_trds::Bool=true,
                                   del_non_secondary_trds::Bool=true,
                                   del_special_trds::Bool=true,
                                   del_wap_trds::Bool=true,
                                   del_agu_locked_in_trds::Bool=false,
                                   del_non_corp_trds::Bool=true)

    cond = true
    # Delete WI trades
    if del_wi_trds
      println("Deleting trades executed on an 'When Issued' basis...")
      wi_cond = ismissing.(df[:, :wis_fl]) .| (df[:, :wis_fl] .!= "N")
      # df = df[wi_cond .== false, :]
      cond = .&(cond, .!wi_cond)
    end

    # Delete trades that took place outside secondary markets
    if del_non_secondary_trds
      println("Keeping only secondary market trades and primary market trades executed at a market price...")
      osm_cond = ismissing.(df[:, :trdg_mkt_cd]) .| (df[:, :trdg_mkt_cd] .!= "S1")
      # df = df[osm_cond .== false, :]
      cond = .&(cond, .!osm_cond)
    end

    # Delete trades executed under special circumstances (Special Price Modifier)
    if del_special_trds
      println("Deleting trades executed under special circumstances...")
      spcl_cond = .&(ismissing.(df[:, :spcl_trd_fl]) .== false, df[:, :spcl_trd_fl] .== "Y")
      # df = df[spcl_cond .== false, :]
      cond = .&(cond, .!spcl_cond)
    end

    # Delete trades where the price is a weighted average
    if del_wap_trds
      println("Deleting trades where the price is a weighted average...")
      wa_cond1 = .&(ismissing.(df[:, :scrty_type_cd]) .== false, df[:, :scrty_type_cd] .== "W") # pre-2012
      wa_cond2 = .&(ismissing.(df[:, :trd_mod_4]) .== false, df[:, :trd_mod_4] .== "W")        # post-2012
      # df = df[.&(.!wa_cond1, .!wa_cond2), :]
      cond = .&(cond, .&(.!wa_cond1, .!wa_cond2))
    end

    # Delete AGU/Locked-In Trades
    if del_agu_locked_in_trds
      println("Deleting Automatic Give-Up (AGU)/Locked-in Trades...")
      agu_cond = .&(ismissing.(df[:, :agu_qsr_id]) .== false,
                    (df[:, :agu_qsr_id] .== "A") .| (df[:, :agu_qsr_id] .== "Q")) # pre-2012
      li_cond = .&(ismissing.(df[:, :lckd_in_ind]) .== false, df[:, :lckd_in_ind] .== "Y")
      cond = .&(cond, .&(.!agu_cond, .!li_cond))
    end

    if del_non_corp_trds
  #     # Delete trades of equity linked notes
  #     println("Deleting trades of equity linked notes...")
  #     eln_cond1 = .&(ismissing.(df[:, :scrty_type_cd]) .== false, df[:, :scrty_type_cd] .== "E") # pre-2012
  #     eln_cond2 = ismissing.(df[:, :sub_prdct]) .| (df[:, :sub_prdct] .== "ELN")                # post-2012
  #     df = df[.&(.!eln_cond1, .!eln_cond2), :]

      # Non-Corporate
      nc_cond1 = .&(ismissing.(df[:, :scrty_type_cd]) .== false, df[:, :scrty_type_cd] .!= "C") # pre-2012
      nc_cond2 = ismissing.(df[:, :sub_prdct]) .| (df[:, :sub_prdct] .!= "CORP")                # post-2012
      cond = .&(cond, .&(.!nc_cond1, .!nc_cond2))
    end

    return df[cond, :]
end
# }}}
# Filter Odd Settlement Dates {{{3
function filter_days_to_settlement(df::DataFrame)
    # Approximately 97% of the trades are settled
    # within 5/6 days from execution in 2013.

    # DAYS TO SETTLEMENT
    # If days to settlement is very non-standard then
    # delete it (6 is arbitrary). From a certain date the
    # settlement date is given instead of the days to settle;
    dts_cond1 = .&(ismissing.(df[:, :days_to_sttl_ct]) .== false,
                   df[:, :days_to_sttl_ct] .> 6) # pre-2012

    # Compute Days to Settlement
    df[!, :trd_exctn_date] .= Date.(df[:, :trd_exctn_dt], dateformat"yyyymmdd")
    df[!, :stlmnt_date] .= Date.(string.(df[:, :stlmnt_dt]), dateformat"yyyymmdd")
    dts_cond2 =  (df[!, :stlmnt_date] .- df[!, :trd_exctn_date]) .> Dates.Day(6)

    # Filter
    println("Deleting trades that were settled more than 6 days after execution...")
    df = df[.&(.!dts_cond1, .!dts_cond2), :]

    return df
end
# }}}
# }}}
# MERGENT {{{1
# Ratings Dataset {{{2
# Get column types
function return_ratings_type(x::String)
    if any([occursin(y, x) for y in ["DATE", "MATURITY", "_ID"]])
        return Int64
    else
        return String
    end
end

# Load MERGENT FISD - Bond Ratings Data
function load_mfisd_ratings(dto; filter_rating::Bool=true,
                            rating_types::Array{String, 1}=["MR", "SPR"])
    # File Path Name
    fpname = string(dto.mf.mergent_path, "/", dto.mf.mergent_file_prefix, "_ratings.csv")

    # Get Column Types
    rdfcl = names(DataFrame!(CSV.File(fpname; limit=1)))           # column names
    rdf_cols = Dict{String, DataType}([x => return_ratings_type(x) for x in rdfcl])

    # Load MERGENT FISD - Ratings Data
    rdf = DataFrame!(CSV.File(fpname; types=rdf_cols))

    if filter_rating
        return rdf[findall(in(rating_types), rdf[:, :RATING_TYPE]), :]
    end

    return rdf
end
# }}}
# BOND ISSUES Dataset {{{2
# Get column types
function return_issues_type(x::String)
    if any([(x in ["REALLOWANCE", "SELLING_CONCESSION"]),
            any([occursin(y, x) for y in ["SPREAD", "YIELD", "AMT", "PRICE"]])])
       return Float64
   elseif x in ["TREASURY_MATURITY", "DATE_SUBJ_ADJUSTMENT"]
       return String
   elseif any([occursin(y, x) for y in ["DATE", "MATURITY", "_ID"]])
        return Int64
    else
        return String
    end
end

function load_mfisd_bi(dto;
                       rmnp::Bool=true,
                       min_maturity::Int64=10^8)
    # File Path Name
    fpname = string(dto.mf.mergent_path, "/", dto.mf.mergent_file_prefix, "_issues.csv")

    # Get Column Types
    idfcl = names(DataFrame!(CSV.File(fpname; limit=1)))       # column names
    idf_cols = Dict{String, DataType}([x => return_issues_type(x) for x in idfcl])

    # Load MERGENT FISD - Bond Issues Data
    df = @time DataFrame!(CSV.File(fpname; types=idf_cols))

    # Remove Non-Perpetual Bonds with Missing Maturity DateA
    cond1 = true
    if rmnp
        println("Removing non-perpetual bonds with missing maturity date...")
        cond1 = .&(df[:, "PERPETUAL"] .!== "Y",
                   df[:, "MATURITY"] .=== missing) .== false
    end

    # Keep only if maturity after 2011-12-31
    cond2 = true
    if min_maturity < 10^8
        println(string("Removing bonds with maturity date before or at ",
                       min_maturity, "..."))
        cond2 = .&(df[:, :MATURITY] .!== missing, df[:, :MATURITY] .> min_maturity)
    end

    if size(.&(cond1, cond2), 1) == 1
        return df
    end

    return df[.&(cond1, cond2), :]
end
# }}}
# MERGE FISD DFs {{{2
function merge_mfisd_issues_ratings(idf::DataFrame, rdf::DataFrame;
                                    bond_id::Symbol=:ISSUE_ID,
                                    eval_merge::Bool=true)
    irdf = leftjoin(idf, rdf; on=bond_id, makeunique=true)

    if eval_merge
        icusips = unique(idf[:, bond_id])
        rcusips = unique(rdf[:, bond_id])

        # CUSIPS in Bond Issues DF and not in Ratings DF
        imr_id = setdiff(idf[:, bond_id], rdf[:, bond_id])

        # CUSIPS in Ratings DF and not in Bond Issues DF
        rmi_id = setdiff(rdf[:, bond_id], idf[:, bond_id])

        # Find index of rows in Ratings DF of CUSIPS not in Bond Issues DF
        rmi_index = findall(in(rmi_id), rdf[:, bond_id])

        # Check that left join was succesfull:
        target = size(imr_id, 1) + (size(rdf, 1) - size(rmi_index, 1))

        # Evaluate
        merge_status = target == size(irdf, 1)
        msg = merge_status ? "success!" : "fail!"
        println(string("MERGE Status: ", msg))

        if !merge_status
            return
        end
    end

    return sort(irdf, [bond_id, :RATING_DATE])
end
# }}}
# FINAL MERGENT FISD {{{2
function mergent_rating_filter(dto, mdf::DataFrame;
                               rating_types::Array{String, 1}=["MR", "SPR"])

    res = Dict{Symbol, Any}()

    # TRACE DATA
    # res[:tr_cusips] = unique(tdf[:, :cusip_id])
    # println(string("Unique CUSIPs in TRACE file: ", size(res[:tr_cusips], 1)))

    # MERGENT FISD DATA
    res[:mf_cusips] = unique(mdf[:, :COMPLETE_CUSIP])
    res[:mrperc] = count(mdf[:, :RATING_TYPE] .=== missing)/size(mdf, 1)
    println(string("Number of observations (rows) in MERGENT FISD file: ", size(mdf, 1)))
    println(string("Unique CUSIPs in MERGENT FISD file: ", size(res[:mf_cusips], 1)))
    println(string("Percentage of observations with missing rating type data: ", res[:mrperc]))

    # Filter Ratings
    fmdf = mdf[findall(in(rating_types), mdf[:, :RATING_TYPE]), :]
    res[:fmf_cusips] = unique(fmdf[:, :COMPLETE_CUSIP])
    res[:fmrperc] = count(fmdf[:, :RATING_TYPE] .=== missing)/size(fmdf, 1)
    println(string("Number of observations (rows) in MERGENT FISD file after filtering ratings: ", size(fmdf, 1)))
    println(string("Unique CUSIPs in MERGENT FISD file after filtering ratings: ", size(res[:fmf_cusips], 1)))
    println(string("Percentage of observations with missing rating type data after filtering ratings: ", res[:fmrperc]))

    # Relative Size (Before v.s. After Rating Type Filtering)
    res[:fnf_rows_ratio] = size(fmdf, 1)/ size(mdf, 1)
    println(string("Filtered v.s. Non-Filtered DF size ratio (nrows): ", res[:fnf_rows_ratio]))

    # Unique CUSIPs (Before v.s. After Rating Type Filtering)
    res[:fnf_cusips_ratio] = size(res[:fmf_cusips], 1)/size(res[:mf_cusips], 1)
    println(string("Filtered v.s. Non-Filtered DF unique cusips ratio: ", res[:fnf_cusips_ratio]))

    return fmdf, res
end

function get_mergent_fisd_df(dto;
                             rmnp::Bool=true,
                             min_maturity::Int64=10^8,
                             filter_rating::Bool=true,
                             rating_types::Array{String, 1}=["MR", "SPR"],
                             bond_id::Symbol=:ISSUE_ID,
                             eval_merge::Bool=true,
                             miss_date::Int64=11111111)

    idf = load_mfisd_bi(dto; rmnp=rmnp, min_maturity=min_maturity)
    rdf = load_mfisd_ratings(dto; filter_rating=false,
                             rating_types=rating_types)

    # NOTE: the availability of rating data does not
    # affect the number of unique cusips in the data:
    # Since we perform an inner merge of the
    # BOND ISSUE DF with the RATINGS DF, the final
    # dataframe has the same rows as the BOND ISSUE DF.
    mdf =  merge_mfisd_issues_ratings(idf, rdf;
                                      bond_id=bond_id,
                                      eval_merge=eval_merge)

    if filter_rating
        mdf, _ = mergent_rating_filter(dto, mdf;
                                       rating_types=rating_types)
    end

    # Dealing with Missing Dates
    mdf[:, :rating_date] .= mdf[!, :RATING_DATE]

    # Convert to Date Date
    date_format = Dates.DateFormat("yyyymmdd")
    mdf[mdf[:, :rating_date] .=== missing, :rating_date] .= miss_date
    mdf[!, :rating_date] .= Dates.Date.(string.(mdf[!, :rating_date]), date_format)

    return mdf
end
# }}}
# LOAD FILTERED MERGENT {{{2
function load_mergent_filtered_df(dto; drop_cols::Bool=true)
    mdf_fpath = dto.mf.mergent_path
    mdf_fname = string("mergent_", dto.mf.filter_file_prefix, ".csv")
    fpath_name = string(mdf_fpath, "/", mdf_fname)

    # # Date Columns
    # dcd = Dict{Symbol,DataType}([x => String for x in mergent_vars_keep
    #                              if occursin("_date", string(x))])

    # # BOOL COLUMNS
    # bcd = Dict{Symbol,DataType}([x => Bool for x in convert2bool_cols])

    # # BOND RATINGS COLUMNS
    # rcd = Dict{Symbol,DataType}([x => return_ratings_type(string(x)) for x in mergent_vars_keep
    #                              if .&(x ∉ convert2bool_cols, x ∉ keys(dcd), occursin("RAT", string(x)))])

    # # BOND ISSUE COLUMNS
    # icd = Dict{Symbol,DataType}([x => return_issues_type(string(x)) for x in mergent_vars_keep
    #                              if .&(x ∉ convert2bool_cols, x ∉ keys(dcd), x ∉ keys(rcd))])

    # # Column types
    # colsd = merge(dcd, bcd, rcd, icd)


    # Get Column Types ======================================================
    dfcl = names(DataFrame!(CSV.File(fpath_name; limit=1)))       # column names
    # Exclude user-defined boolean columns
    bool_cols = [x for x in user_defined_keep if x != :rating_date] 
    # Remaining columns
    rdfcl = [x for x in dfcl if !(x in bool_cols)]
    # Get types of remaining columns
    colsd = Dict{String, DataType}([x => return_issues_type(x) for x in rdfcl])
    # Collect all types:
    colsd = merge!(colsd, Dict{String, DataType}([string(x) => Bool for x in bool_cols]))
    # =======================================================================
    
    println(string("Loading mergent_",  dto.mf.filter_file_prefix, " dataframe..."))
    mdf = DataFrame!(CSV.File(fpath_name, types=colsd))

    # Convert to date:
    date_format = Dates.DateFormat("yyyy-mm-dd")
    # for col in keys(dcd)
    for col in [x for x in names(mdf) if occursin("date", x)]
        mdf[!, col] .= Dates.Date.(mdf[!, col], date_format)
    end

    if drop_cols
        cols = [x for x in DataMod.mergent_vars_keep if 
                !(x in vcat(DataMod.bondh_prot_cols, 
                            DataMod.convertible_cols, 
                            DataMod.convertible_add_cols))]
        mdf = mdf[:, cols]
    end

    return mdf
end
# }}}
# }}}
# MERGE TRACE and MERGENT {{{1
# PART 1 - MERGE INDEXES {{{2
function merge_trace_mergent_index(tdf::DataFrame, mdf::DataFrame)
    # Inputs
    date_format = Dates.DateFormat("yyyymmdd")
    miss_date = Dates.Date("11111111", date_format)
    ff(t, r) = !isempty(r[t .>= r]) ? maximum(r[t .>= r]) : miss_date

    # Remove duplicate rows
    df1 = unique(tdf[:, [:cusip_id, :trd_exctn_date]])     # trace
    df2 = unique(mdf[:, [:COMPLETE_CUSIP, :rating_date, :RATING_TYPE]])  # mergent

    println("Step 1. Merging ID DataFrames...")
    idf = innerjoin(df1, df2, on = :cusip_id => :COMPLETE_CUSIP)

    println("Step 2. By CUSIP and Trade Execution Date, find the last Rating Date... ")
    tmp = combine(groupby(idf, [:cusip_id, :trd_exctn_date]),
                  [:trd_exctn_date, :rating_date] => ((t, r) -> ff(t, r)) => :last_rating_date)

    # Drop if last rating date does not exist
    tmp = tmp[tmp[:, :last_rating_date] .> miss_date, :]

    println("Step 3. Filter: for each (cusip, trade execution date) pair, keep only the last rating date...")
    idf = innerjoin(idf, tmp, on = [:cusip_id, :trd_exctn_date])
    idf = idf[idf[:, :rating_date] .== idf[:, :last_rating_date], Not(:last_rating_date)]

    println("Step 4. Filter: for the (cusip, trade execution date, rating_date) tuples with \n",
            "        more than one RATING_TYPE observation, keep only the Moody's rating..." )
    # Count the number of rating type observations by unique
    # (cusip, trade execution date, rating_date) entries:
    gcols = [x for x in Symbol.(names(idf)) if (x != :RATING_TYPE)]
    tmp = sort!(combine(groupby(idf, gcols), nrow), :nrow, rev=true)

    # Add the row count to the index dataframe
    idf = DataFrames.leftjoin(idf, tmp, on=[:cusip_id, :trd_exctn_date, :rating_date])

    # For the (cusip, trd_exct_date, rating_date) tuples with more than one
    # rating_type observation, keep only the Moody's rating:
    cond = .&(idf[:, :nrow] .> 1, idf[:, :RATING_TYPE] .== "SPR")
    idf = idf[.!cond, Not(:nrow)]

    # %% Confirm there is no entry with more than 1 observation
    # tmp = sort!(combine(groupby(idf, gcols), nrow), :nrow, rev=true)
    # println(unique(tmp2[:, :nrow]))

    return idf
end
# }}}
# PART 2 - DIAGNOSTICS {{{2
function merge_diagnosis(tdf::DataFrame, mdf::DataFrame, idf::DataFrame)
    # %% idf is the mergent index dataframe computed by
    # function merge_trace_mergent_index above.

    # %% Compute Unique CUSIPS in the DataFrames
    tr_cusips = unique(tdf[:, :cusip_id])
    mf_cusips = unique(mdf[:, :COMPLETE_CUSIP])
    println(string("Unique CUSIPS in trace dataframe: ", size(tr_cusips, 1)))

    # Find Common CUSIPS
    tim_cusips = intersect(tr_cusips, mf_cusips)

    # %% See ratio of cusips in trace that can be matched to
    #    MERGENT FISD cusips
    match_ratio = (size(tim_cusips, 1)/size(tr_cusips, 1)) * 100
    println(string("TRACE/MERGENT Matched CUSIPS Ratio (%): ", match_ratio))

    # Out of the cusips matched, compute the unique (cusip, trd exctn date) pairs
    idx_cusips = findall(in(tim_cusips), tdf[:, :cusip_id])

    # Compute how many (cusip, trd exctn date) pairs are lost in merging process
    # (=> the merging process requires rating data to be available)
    tmp = unique(tdf[idx_cusips, [:cusip_id, :trd_exctn_date]])
    # Success Rate (conditional on initial match):
    cond_success_rate = (size(idf, 1) ./size(tmp, 1)) * 100
    println(string("Conditional Success Rate (%): ", cond_success_rate))

    return DataFrame(:match_ratio => match_ratio,
                     :cond_success_rate => cond_success_rate)
end
# }}}
# MAIN - MERGE TRACE AND MERGENT {{{2
function merge_trace_mergent_dfs(dto, tdf::DataFrame, mdf::DataFrame;
                                 mcols=Array{String, 1}[])

    if !isempty(mcols)
        mdf=mdf[:, mcols]
    end

    idf = @time merge_trace_mergent_index(tdf, mdf)

    # Run diagnosis
    mdiag = merge_diagnosis(tdf, mdf, idf)

    # Merge DataFrames
    fdf = DataFrames.innerjoin(tdf, idf, on=[:cusip_id, :trd_exctn_date])
    fdf = DataFrames.innerjoin(fdf, mdf, on=[:cusip_id => :COMPLETE_CUSIP, :rating_date, :RATING_TYPE])

    return fdf, mdiag
end
# }}}
# FINAL - MERGENT FISD Analysis {{{2
function count_rows(mdf::DataFrame, id_cols::Array{Symbol,1};
                    min_date::Date=Dates.Date("20111231", date_format))

        return sort!(combine(groupby(mdf[mdf[:, :rating_date] .> min_date, :],
                                     id_cols), nrow), :nrow, rev=true)
end

# Investigating why COMPLETE_CUSIP and RATING_DATE are not sufficient
# to uniquely identify entries in the MERGENT FISD DataFrames
function find_multiple_occurrences(mdf::DataFrame, id_cols::Array{Symbol,1};
                                   min_date::Date=Dates.Date("20111231", date_format))

     tmp = count_rows(mdf, id_cols; min_date=min_date)

     # Step 1. Pick the (CUSIP, Rating Date) pair with the greater number of rows:
     tcond = .&(mdf[:, :COMPLETE_CUSIP] .== tmp[1, :COMPLETE_CUSIP],
                mdf[:, :rating_date] .== tmp[1, :rating_date])
     tmp2 = mdf[tcond, :]

     # Step2. Now, let's find the columns with non-unique values:
     # Columns not to be stacked
     nscols = [:COMPLETE_CUSIP, :rating_date]
     tmp2 = stack(tmp2, Not(nscols))

     # Step 3. Count the number of non-unique entries by variable:
     tmp2 = sort!(combine(groupby(unique(tmp2), Not(:value)), nrow), :nrow, rev=true)
     return tmp2[tmp2[:, :nrow] .> 1, :]
end
# }}}
# }}}
# ANALYSIS {{{1
# IG and HY {{{2
function ig_classifier(rt::String, type::String)
    ig = []
    if type == "MR"
        # Moody's IG ratings
        ig = vcat("Aaa", [string(x, n) for n in 1:3, x in ["Aa", "A"]]...,
                  [string("Baa", n) for n in 1:3]...)
    elseif type == "SPR"
        # S&P's IG Ratings
        ig = vcat("AAA", [string(x, s) for s in ["+", "", "-"], x in ["AA", "A"]]...,
                  [string("BBB", s) for s in ["+", "", "-"]]...)
    else
        println("Error! Rating type not recognized. \n",
                "Please enter type = 'MR' or 'SPR'. Exiting...")
        return
    end

    return (rt in ig)
end
# }}}
# AGE and TTM {{{2
function compute_age_ttm(df::DataFrame; basis=DayCounts.Actual360())
     date_format = Dates.DateFormat("yyyymmdd")
     df[!, :issuance_date] .= Dates.Date.(string.(df[!, :OFFERING_DATE]), date_format)
     df[!, :maturity_date] .= Dates.Date.(string.(df[!, :MATURITY]), date_format)

     year_frac_fun = (x, y) -> DayCounts.yearfrac(x, y, basis)
     df[!, :age] = year_frac_fun.(df[!, :issuance_date], df[!, :trd_exctn_date])
     df[!, :ttm] = year_frac_fun.(df[!, :trd_exctn_date], df[!, :maturity_date])
     # df[!, :age] .= Dates.value.(df[!, :trd_exctn_dt] .- df[!, :issuance_dt])./ 365
     # df[!, :ttm] .= Dates.value.(df[!, :maturity_dt ] .- df[!, :trd_exctn_dt])./ 365

     return df
end
# }}}
# TRADE SIZE CATEGORIES {{{2
# Group 1: 0-100k
# Group 2: 100k-1M
# Group 3: 1M-5M
# Group 4: 5M-10M
# Group 5: 10M-25M
# Group 6: 25M+
function compute_trade_size_cats(df::DataFrame)
    upper_vec = [1e5, 1e6, 5e6, 1e7, 2.5e7]
    df[!, :volg1] .= df[:, :entrd_vol_qt] .<= upper_vec[1]
    df[!, :volg2] .= .&(df[:, :entrd_vol_qt] .> upper_vec[1],
                        df[:, :entrd_vol_qt] .<= upper_vec[2])
    df[!, :volg3] .= .&(df[:, :entrd_vol_qt] .> upper_vec[2],
                        df[:, :entrd_vol_qt] .<= upper_vec[3])
    df[!, :volg4] .= .&(df[:, :entrd_vol_qt] .> upper_vec[3],
                        df[:, :entrd_vol_qt] .<= upper_vec[4])
    df[!, :volg5] .= .&(df[:, :entrd_vol_qt] .> upper_vec[4],
                        df[:, :entrd_vol_qt] .<= upper_vec[5])
    df[!, :volg6] .= df[:, :entrd_vol_qt] .> upper_vec[5]
    
    return df
end
# }}}
# Create Stats variables {{{2
function create_stats_vars(df::DataFrame)
    # IG Indicator
    println("Creating IG indicator...")
    df[!, :ig] .= map(x -> ig_classifier(x.RATING, x.RATING_TYPE), eachrow(df))

    # AGE and TTM
    println("Creating AGE and TTM variables...")
    df = compute_age_ttm(df)

    # Trade Size Categories
    println("Creating TRADE SIZE Categories...")
    df = compute_trade_size_cats(df)

    # ATS Indicator (trace variable is either missing or "Y")
    println("Creating ATS indicator...")
    df[!, :ats] = df[:, :ats_indicator] .!== missing

    return df
end
# }}}
# COVENANT Groups {{{2
function cov_cond(df::DataFrame, var::Symbol)
    nonboolcov_str = [:RATING_DECLINE_PROVISION,
                      :VOTING_POWER_PERCENTAGE,
                      :DECLINING_NET_WORTH_PROVISIONS,
                      :VOTING_POWER_PERCENTAGE_ERP,
                      :LEVERAGE_TEST_SUB, 
                      :DECLINING_NET_WORTH_PERCENTAGE, 
                      :DECLINING_NET_WORTH_TRIGGER]
    nonboolcov_num = [ ]

    if var in nonboolcov_str
        return .&(df[!, var] .!== missing, df[!, var] .!= "0")
    elseif var in nonboolcov_num
        return .&(df[!, var] .!== missing, df[!, var] .> .0)
    end

    return .&(df[:, var] .!== missing, df[:, var] .== "Y")
end

# Create Covenant Groups
function covgr_indicators(df::DataFrame)
    for grn in keys(DataMod.covgr)
        col = Symbol(:cg, grn)
        vars = [x for x in keys(vars2covgr) if vars2covgr[x] == grn]

        df[!, col] = .|([cov_cond(df, var) for var in vars]...)    
    end

    return df
end
# }}}
# }}}
# MODULE END =============================================================================
end
