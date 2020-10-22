# vim: set fdm=marker :

module StatsMod

using CSV
using DataFrames
using Distributed
using Dates
using Statistics

#  STATS  {{{1
# Trace Stats (Bond Count, Trade Count, Trade Volume) {{{2
function stats_calculator(df::DataFrame, groups::Array{Symbol,1};
                          var_suffix::Symbol=Symbol(""))
    cols = vcat(groups, [:cusip_id, :entrd_vol_qt])
    gd = groupby(df[:, cols],  groups)
    cdf = combine(gd, :entrd_vol_qt =>  (x -> Statistics.quantile!(x, .25)) => :qt25_trd_vol,
                      :entrd_vol_qt => mean => :mean_trd_vol,
                      :entrd_vol_qt => median => :median_trd_vol,
                      :entrd_vol_qt => (x -> Statistics.quantile!(x, .75)) => :qt75_trd_vol,
                      :entrd_vol_qt => (x -> sum(x)/ 10^9) => :total_vol_tr,
                      nrow => :trd_count, :cusip_id => (x -> size(unique(x), 1)) => :cusips)

    if !isempty(string(var_suffix))
        col_names = [(x in groups) ? x : Symbol(x, var_suffix) for x in Symbol.(names(cdf))]
        rename!(cdf, col_names)
    end

    return cdf
end

function trace_stats(df::DataFrame)
    df[!, :ats_ind] = df[:, :ats_indicator] .!== missing
    groups = [:trd_exctn_yr, :trd_exctn_mo, :ats_ind, :ig]
    cols = vcat(groups, [:cusip_id, :entrd_vol_qt])

    # BY SECONDARY MARKET & RATING
    # Create stats by year/month/ats/ig indicator
    cdf = @time stats_calculator(df, groups)

    # By SECONDARY MARKET
    # Create stats by year/month/ats indicator
    g1 = [x for x in groups if x != :ig]
    tmp = stats_calculator(df, g1; var_suffix=:_sm)
    cdf = leftjoin(cdf, tmp, on = g1)
    cdf[!, :total_vol_perc_sm] = (cdf[!, :total_vol_tr] ./ cdf[!, :total_vol_tr_sm]) .* 100
    cdf[!, :trd_count_perc_sm] = (cdf[!, :trd_count] ./ cdf[!, :trd_count_sm]) .* 100

    # Create stats by year/month
    tmp = combine(groupby(cdf, [:trd_exctn_yr, :trd_exctn_mo]),
                  :total_vol_tr => sum => :total_vol_all,
                  :trd_count => sum => :trd_count_all)
    cdf = leftjoin(cdf, tmp, on = [:trd_exctn_yr, :trd_exctn_mo])
    cdf[!, :total_vol_perc] = (cdf[!, :total_vol_tr] ./ cdf[!, :total_vol_all]) .* 100
    cdf[!, :trd_count_perc] = (cdf[!, :trd_count] ./ cdf[!, :trd_count_all]) .* 100

    # Sort rows and reorder columns
    trd_cols = [:trd_count, :trd_count_perc,
                :trd_count_sm, :trd_count_perc_sm]
    vol_cols = [:total_vol_tr, :total_vol_perc,
                :total_vol_tr_sm, :total_vol_perc_sm]
    g2cols = [:qt25_trd_vol, :mean_trd_vol, :median_trd_vol, :qt75_trd_vol]
    g1cols = [Symbol(x, :_sm) for x in g2cols]
    col_order=vcat(groups, [:cusips, :cusips_sm],
                   trd_cols, vol_cols,
                   g2cols, g1cols)

    return sort!(cdf[:, col_order], groups)
end
# }}}2
# INDICATORS {{{2
function convert_2_bool(df::DataFrame, x)
    return .&(df[:, x] .!== missing, df[:, x] .== "Y")
end
# }}}2
# CUSIPS stats by ATS/iOTC and IG/HY {{{2
function smk_rt_cov_indicators(x)
   ats = sum(x[:, :ats] .== 1) .> 0
   otc = sum(x[:, :ats] .== 0) .> 0

   # Group 1: ATS, OTC or both
   ats_only = .&(ats, !otc)
   otc_only = .&(!ats, otc)
   ats_otc = .&(ats, otc)

   # Group 2: IG v.s. HY
   ig = sum(x[:, :ig] .== 1) .> 0
   hy = sum(x[:, :ig] .== 0) .> 0

   # Group 3: covenant v.s. no covenant
   cov = unique(x[:, :covenant])[1]

   df1 = DataFrame([:ats_only => ats_only, :otc_only => otc_only,
                    :ats_otc => ats_otc, :ig => ig, :hy => hy,
                    :cov => cov, :ncov => .!cov,
                    :ats_only_ig => .&(ats_only, ig), :otc_only_ig => .&(otc_only, ig),
                    :ats_otc_ig => .&(ats_otc, ig),
                    :ats_only_hy => .&(ats_only, hy), :otc_only_hy => .&(otc_only, hy),
                    :ats_otc_hy => .&(ats_otc, hy),
                    :ats_only_cov => .&(ats_only, cov), :otc_only_cov => .&(otc_only, cov),
                    :ats_otc_cov => .&(ats_otc, cov),
                    :ats_only_ncov => .&(ats_only, .!cov), :otc_only_ncov => .&(otc_only, .!cov),
                    :ats_otc_ncov => .&(ats_otc, .!cov),
                    :ig_cov => .&(ig, cov), :ig_ncov => .&(ig, .!cov),
                    :hy_cov => .&(hy, cov), :hy_ncov => .&(hy, .!cov),
                    :ats_only_ig_cov => .&(ats_only, ig, cov), :otc_only_ig_cov => .&(otc_only, ig, cov),
                    :ats_otc_ig_cov => .&(ats_otc, ig, cov),
                    :ats_only_hy_cov => .&(ats_only, hy, cov), :otc_only_hy_cov => .&(otc_only, hy, cov),
                    :ats_otc_hy_cov => .&(ats_otc, hy, cov),
                    :ats_only_ig_ncov => .&(ats_only, ig, .!cov), :otc_only_ig_ncov => .&(otc_only, ig, .!cov),
                    :ats_otc_ig_ncov => .&(ats_otc, ig, .!cov),
                    :ats_only_hy_ncov => .&(ats_only, hy, .!cov), :otc_only_hy_ncov => .&(otc_only, hy, .!cov),
                    :ats_otc_hy_ncov => .&(ats_otc, hy, .!cov)])


   # df2 = DataFrame(:ats_vol => sum(x[:, :ats] .* x[:, :entrd_vol_qt]),
   #                 :otc_vol => sum(.!x[:, :ats] .* x[:, :entrd_vol_qt]),
   #                 :ats_ig_vol => sum(x[:, :ats] .* x[:, :ig] .* x[:, :entrd_vol_qt]),
   #                 :ats_hy_vol => sum(x[:, :ats] .* .!x[:, :ig] .* x[:, :entrd_vol_qt]),
   #                 :otc_ig_vol => sum(.!x[:, :ats] .* x[:, :ig] .* x[:, :entrd_vol_qt]),
   #                 :otc_hy_vol => sum(.!x[:, :ats] .* .!x[:, :ig] .* x[:, :entrd_vol_qt]),
   #                 :ats_cov_vol => sum(x[:, :ats] .* x[:, :covenant] .* x[:, :entrd_vol_qt]),
   #                 :ats_ncov_vol => sum(x[:, :ats] .* .!x[:, :covenant] .* x[:, :entrd_vol_qt]),
   #                 :otc_cov_vol => sum(.!x[:, :ats] .* x[:, :covenant] .* x[:, :entrd_vol_qt]),
   #                 :otc_ncov_vol => sum(.!x[:, :ats] .* .!x[:, :covenant] .* x[:, :entrd_vol_qt]),
   #                 :ig_cov_vol => sum(x[:, :ig] .* x[:, :covenant] .* x[:, :entrd_vol_qt]),
   #                 :hy_cov_vol => sum(.!x[:, :ig] .*  x[:, :covenant] .* x[:, :entrd_vol_qt]),
   #                 :ig_ncov_vol => sum(x[:, :ig] .*  .!x[:, :covenant] .* x[:, :entrd_vol_qt]),
   #                 :hy_ncov_vol => sum(.!x[:, :ig] .* .!x[:, :covenant] .* x[:, :entrd_vol_qt]), )

    # Sum inner product function:
    sip(z...) = sum(.*(z...))

    # Varables
    va = x[:, :ats]
    vi = x[:, :ig]
    vc = x[:, :covenant]
    vv = x[:, :entrd_vol_qt]

    df2 = DataFrame(:ats_vol => sip(va, vv), 
                    :otc_vol => sip(.!va, vv),
                    :ats_ig_vol => sip(va, vi, vv),
                    :ats_hy_vol => sip(va, .!vi, vv),
                    :otc_ig_vol => sip(.!va, vi, vv),
                    :otc_hy_vol => sip(.!va, .!vi, vv), 
                    :ats_cov_vol => sip(va, vc, vv), 
                    :ats_ncov_vol => sip(va, .!vc, vv),
                    :otc_cov_vol => sip(.!va, vc, vv), 
                    :otc_ncov_vol => sip(.!va, .!vc, vv),
                    :ig_cov_vol => sip(vi, vc, vv), 
                    :hy_cov_vol => sip(.!vi, vc, vv),
                    :ig_ncov_vol => sip(vi, .!vc, vv),
                    :hy_ncov_vol => sip(.!vi, .!vc, vv))

   # df2 = DataFrame([Symbol(y, :_vol) => df1[:, y] .* sum(x[:, :entrd_vol_qt])
   #                  for y in Symbol.(names(df1))])
   #
   return hcat(df1, df2)
end


function compute_indicators(df::DataFrame;
                            zvars::Dict{Symbol, Symbol}=Dict{Symbol, Symbol}(:entrd_vol_qt => :vol))
    df[!, :ats] = df[!, :ats_ind]
    df[!, :otc] = .!df[!, :ats_ind]
    df[!, :ig] = df[!, :ig_ind]
    df[!, :hy] = .!df[!, :ig_ind]
    df[!, :cov] = df[!, :cov_ind]
    df[!, :ncov] = .!df[!, :cov_ind]

    # Variable Combinations
    v1 = Array([[x] for x in [:ats, :otc, :ig, :hy, :cov, :ncov]])
    v2 = reshape([[x, y] for x in [:ats, :otc], y in [:ig, :hy]], 4, 1  )
    v3 = reshape([[x, y] for x in [:ats, :otc], y in [:cov, :ncov]], 4, 1)
    v4 = reshape([[x, y] for x in [:ig, :hy], y in [:cov, :ncov]], 4, 1)
    v5 = reshape([[x, y, z] for x in [:ats, :otc], y in [:ig, :hy], z in [:cov, :ncov]], 8, 1)
    vv = vcat(v1, v2, v3, v4, v5)

    for v in vv
        # Compute Boolean Arrays
        df[!, join(v, :_)] = .*([df[!, x] for x in v]...)

        # Compute Conditional Values
        if !isempty(zvars)
            for zk in keys(zvars)
                df[!, Symbol(join(v, :_), :_, get(zvars, zk, :na))] = .*([df[!, x] for x in vcat(v, zk)]...)
            end
        end
    end

    return df
end

function stats_by_yr_mo_issuer(df::DataFrame;
                               groups::Array{Symbol,1}=[:trd_exctn_yr, :trd_exctn_mo, :ISSUER_ID, :cusip_id],
                               indicators::Array{Symbol,1}=[:ats_ind, :cov_ind, :ig_ind],
                               zvars::Dict{Symbol, Symbol}=Dict{Symbol, Symbol}(:entrd_vol_qt => :vol))
    # Extract cols
    cols = vcat(groups, indicators, keys(zvars)...)
    adf = df[:, cols]

    # Boolean array indicators (ats/otc, ig/hy, cov/ncov)
    adf = compute_indicators(adf, zvars=zvars)

    # Variable Combinations
    # Boolean Columns
    bcols = [Symbol(x) for x in names(adf) if .&(!occursin("_ind", x),
                                                 !occursin("_vol", x),
                                                 !(Symbol(x) in groups))]
    # Volume Columns
    vcols = [Symbol(x) for x in names(adf) if .&(occursin("_vol", x),
                                                  x != "entrd_vol_qt")]

    # Group by yr, mo, issuer, cusip ====================================
    gd1 = groupby(adf,  groups)

    # Compute # trades and total volume by yr, mo, cusip
    # and (ats/otc, ig/hy, cov/ncov) categories
    adf1 = combine(gd1,
                   [x => count => x for x in bcols], # count trades by yr, mo, cusip
                   [x => sum => x for x in vcols],)  # sum volume by yr, mo, cusip
    # ===================================================================

    # Group by yr, mo, issuer ===========================================
    gd2 = groupby(adf1,  [x for x  in groups if (x != :cusip_id)])

    # Compute # cusips, # trades, and total volume by yr, mo, issuer
    # and (ats/otc, ig/hy, cov/ncov) categories
    return combine(gd2,
                   :cusip_id => (x -> size(unique(x), 1)) => :cusips,
                   [x => (x -> count(x .> 0)) => Symbol(x, :_cusips) for x in bcols],
                   [x => sum => Symbol(x, :_trades) for x in bcols],
                   [x => sum for x in vcols],)
    # ===================================================================
end
# }}}2
# }}}1
# New Analysis {{{1
# Filtering Functions {{{2
struct Filter
  sbm::Symbol
  rating::Symbol
  covenant::Symbol
end

function filter_constructor(sbm::Symbol, rating::Symbol, covenant::Symbol)
    sbm in (:ats, :otc, :both, :any) || error("invalid secondary bond market filter! \n",
                                              "Please enter :ats, :otc, :any or :both")
    rating in (:y, :n, :any) || error("invalid rating filter! \n", "Please enter :y, :n or :any")
    covenant in (:y, :n, :any) || error("invalid covenant filter! \n", "Please enter :y, :n or :any")

    return Filter(sbm, rating, covenant)
end

function gen_sbm_rt_cvt_cat_vars(df::DataFrame)
    sbmf(x, y) = (x == y == 1) ? :both : (x == y) ? :any : (x == 1) ? :ats : :otc
    covenantf(x, y) = (x == y) ? :any : (x == 1) ? :cov : :ncov
    ratingf(x, y) = (x == y) ? :any : (x == 1) ? :ig : :hy

    df[!, :sbm] .= sbmf.(df[:, :ats], df[:, :otc])
    df[!, :rt] .= ratingf.(df[:, :ig], df[:, :hy])
    df[!, :cvt] .= covenantf.(df[:, :cov], df[:, :ncov])

    return df
end

function get_ind_vec(ft)
    fd = Dict{Symbol, Array{Int64,1}}(:ats => [1, 0],
                                      :otc => [0, 1],
                                      :both => [1, 1],
                                      :y => [1, 0],
                                      :n => [0, 1],
                                      :any => [0, 0])

    vcat(fd[ft.sbm], fd[ft.rating], fd[ft.covenant])
end

function get_filter_comb(x1::Symbol, x2::Symbol, x3::Symbol; 
                         co::Array{Symbol,1}=[:ats, :otc, :ig, :hy, :cov, :ncov])
    values = get_ind_vec(filter_constructor(x1, x2, x3))

    return DataFrame([co[i] => values[i] for i in 1:size(co, 1)])
end 

function get_filter_combinations(; co::Array{Symbol,1}=[:ats, :otc, :ig, :hy, :cov, :ncov])
    avals = [:any, :ats, :otc, :both] # for secondary bond market
    vals = [:any, :y, :n] # for ig, covenant, convertible

    return vcat([get_filter_comb(x1, x2, x3; co=co) for x1 in avals, x2 in vals, x3 in vals]...)[:, co]
end 

function dfrow2dict(df::DataFrame, row::Int64)
    cols = [x for x in Symbol.(names(df)) if !(x in [:sbm, :rt, :cvt])]

    return Dict{Symbol, Int64}([cn => df[row, cn] for cn in cols])
end

function get_filter_cond(df::DataFrame, x::Dict{Symbol,Int64})

    # Array of all true
    cond = typeof.(df[:, :ats]) .== Bool

    if .&(x[:ats] == 1, x[:otc] == 1)
        cond = .&(cond, df[:, :bond_ats_otc])
    else
        cond = x[:ats] .== x[:otc] ? cond : .&(cond, df[:, :ats] .== x[:ats])
    end

    cond = x[:ig] .== x[:hy] ? cond : .&(cond, df[:, :ig] .== x[:ig])
    cond = x[:cov] .== x[:ncov] ? cond : .&(cond, df[:, :covenant] .== x[:cov])

    return cond
end
# }}}
# By Number of Covenants {{{2
function get_combination(sbm::Symbol, rt::Symbol; 
                         combdf::DataFrame=DataFrame())

    if isempty(combdf)
        combdf =  StatsMod.get_filter_combinations()
    end
    
    id_cols = [:sbm, :rt, :cvt]
    if any([!(x in Symbol.(names(combdf))) for x in id_cols])
        combdf = StatsMod.gen_sbm_rt_cvt_cat_vars(combdf)
    end
    
    row = argmax(.&(combdf[:, :sbm] .== sbm, 
                    combdf[:, :rt] .== rt, 
                    combdf[:, :cvt] .== :any))
    
    cols = [x for x in Symbol.(names(combdf)) if !(x in id_cols)]
    return StatsMod.dfrow2dict(combdf[:, cols], row)
end

function stats_by_num_cov(df;     
                      groupby_date_cols::Array{Symbol,1}=[:trd_exctn_yr, :trd_exctn_qtr])
    stats_cols = vcat(groupby_date_cols, :sbm, :rt, :cusip_id, :sum_num_cov)
    gdf1 = unique(df[:, stats_cols])
    df1 = combine(groupby(df, vcat(groupby_date_cols, :sbm, :rt)), 
                  :sum_num_cov => (x -> Statistics.median(x)) => :median_num_cov,
                  :sum_num_cov => (x -> Statistics.mean(x)) => :mean_num_cov)

    gdf2 = groupby(df, vcat(groupby_date_cols, :sbm, :rt, :sum_num_cov))
    df2 = combine(gdf2, 
            # Volume Statistics: 
            :entrd_vol_qt => (x -> Statistics.mean(skipmissing(x))) => :mean_vol_by_num_cov,
            :entrd_vol_qt => (x -> Statistics.median(skipmissing(x))) => :median_vol_by_num_cov,
            :entrd_vol_qt => (x -> sum(skipmissing(x))/1e9) => :total_vol_by_num_cov,

            # Trade Count:
            :cusip_id => (x -> size(x, 1)) => :trades_by_num_cov,

            # Number of Bonds:
            :cusip_id => (x -> size(unique(x), 1)) => :bonds_by_num_cov, 

            # Number of issuers:
            :ISSUER_ID => (x -> size(unique(x), 1)) => :issuers_by_num_cov)

    return sort!(innerjoin(df1, df2, on=vcat(groupby_date_cols, :sbm, :rt)),
                 vcat(groupby_date_cols, :sbm, :sum_num_cov))
end

function compute_stats_by_num_cov(df::DataFrame, sbm::Symbol, rt::Symbol, combdf::DataFrame)
    combd = get_combination(sbm, rt; combdf=combdf)
    tmp =  deepcopy(df[StatsMod.get_filter_cond(df, combd), :])
    tmp[!, :sbm] .= sbm
    tmp[!, :rt] .= rt
    
    return StatsMod.stats_by_num_cov(tmp)
end
# }}}
# By Covenant Categories {{{2
function filter_selected(df::DataFrame;
                         date_cols::Array{Symbol, 1}=[:trd_exctn_yr, :trd_exctn_qtr],
                         extra_cols::Array{Symbol, 1}=Symbol[])
    bond_cols = [:ISSUER_ID, :cusip_id]
    filter_cols = vcat([:ats, :ig, :covenant], 
                       [Symbol(x) for x in names(df) if occursin("cg", x)])
    trd_cols = [:entrd_vol_qt]
    cols = vcat(date_cols, bond_cols, filter_cols, trd_cols, extra_cols)

    # Keep only the selected securities 
    df = df[df[:selected], cols]

    # Create indicator for bonds/issuers that trade
    # on both markets in the same period:
    colsl = vcat(date_cols, :cusip_id)
    tmp = combine(groupby(df, colsl), 
                  :ats => (x -> .&(count(x) > 0, count(x .== false) > 0)) => :bond_ats_otc)
    df = innerjoin(df, tmp, on=colsl)

    colsl = vcat(date_cols, :ISSUER_ID)
    tmp = combine(groupby(df, colsl), 
                          :bond_ats_otc => (x -> count(x) > 0) => :issuer_ats_otc)   
    return innerjoin(df, tmp, on=colsl)
end


function vol_stats_generator(gdf)
    return combine(gdf, :entrd_vol_qt => (x -> Statistics.quantile!(x, .25)) => :qt25_trd_vol,
                        :entrd_vol_qt => mean => :mean_trd_vol,
                        :entrd_vol_qt => median => :median_trd_vol,
                        :entrd_vol_qt => (x -> Statistics.quantile!(x, .75)) => :qt75_trd_vol,
                        :entrd_vol_qt => (x -> sum(x)/1e9) => :total_trd_vol_tr)
end

# function cov_group_stats_generator(gdf)
#     cgcols = [Symbol(:cg, x) for x in 1:15]
#     cgvol(x) = sum(.*([getfield(x, col) for col in keys(x)]...))/1e9

#     return combine(gdf, 
#                    [cg => count => Symbol(cg, :_trd_count) for cg in cgcols], 
#                    [Symbol(cg,  :_bonds) => (x -> size(unique(x[.!ismissing.(x)]), 1)) => Symbol(cg, :_bonds) for cg in cgcols], 
#                    [Symbol(cg,  :_issuers) => (x -> size(unique(x[.!ismissing.(x)]), 1)) => Symbol(cg, :_issuers) for cg in cgcols], 
#                    [Symbol(cg,  :_trd_vol) => (x -> sum(x)/1e9) => Symbol(cg, :_trd_vol_tr) for cg in cgcols])
# end

# function stats_generator(df::DataFrame, combd::Dict{Symbol,Int64};
#                          groupbycols::Array{Symbol, 1}=[:trd_exctn_yr, :trd_exctn_qtr])
#     tmpdf = df[get_filter_cond(df, combd), :]

#     cgcols = [Symbol(:cg, x) for x in 1:15]
#     fx(b, x) = b ? x : missing
#     for cg in cgcols
#         tmpdf[!, Symbol(cg, :_trd_vol)] = .*(tmpdf[:, cg], tmpdf[:, :entrd_vol_qt])
#         tmpdf[!, Symbol(cg, :_bonds)] = fx.(tmpdf[:, cg], tmpdf[:, :cusip_id])
#         tmpdf[!, Symbol(cg, :_issuers)] = fx.(tmpdf[:, cg], tmpdf[:, :ISSUER_ID])
#     end

#     gdf = groupby(tmpdf, groupbycols)

#     # Number of bonds and number of trades
#     df1 = combine(gdf, nrow => :total_trd_count,
#                   :cusip_id => (x -> size(unique(x), 1)) => :total_bonds,
#                   :ISSUER_ID => (x -> size(unique(x), 1)) => :total_issuers)

#     # Volume stats
#     df2 = vol_stats_generator(gdf)

#     # Covenant Stats
#     df3 = cov_group_stats_generator(gdf)

#     # Join Stats DataFrames
#     sdf = innerjoin(innerjoin(df1, df2, on=groupbycols), df3, on=groupbycols)

#     # Reorder columns
#     cols1 =vcat(groupbycols, Symbol.(keys(combd)))
#     cols2 = [x for x in Symbol.(names(sdf)) if !(x in cols1)]
#     cols = vcat(cols1, cols2)
#     return hcat(sdf, repeat(DataFrame(combd), inner=size(df2, 1)))[:, cols]
# end

# Statistics by Covenant Categories:
function cg_vol_stats_generator(gdf)
    cgcols = [Symbol(x) for x in names(gdf) if .&(occursin("cg", x), !occursin("_", x))]


    f_mean(x) = !isempty(skipmissing(x)) ? Statistics.mean(skipmissing(x)) : NaN
    f_median(x) = !isempty(skipmissing(x)) ? Statistics.median(skipmissing(x)) : NaN   
    f_vol(x) = !isempty(skipmissing(x)) ? sum(skipmissing(x))/1e9 : NaN   
    return combine(gdf, 
        # Volume Statistics: 
        [Symbol(cg, :_trd_vol) => (x -> f_mean(x))  => 
            Symbol(cg, :_mean_trd_vol) for cg in cgcols],
        [Symbol(cg, :_trd_vol) => (x -> f_median(x)) => 
            Symbol(cg, :_median_trd_vol) for cg in cgcols],
        [Symbol(cg, :_trd_vol) => (x -> f_vol(x)) => 
            Symbol(cg, :_trd_vol_tr) for cg in cgcols],
        
        # Trade Count:
        [cg => count => Symbol(cg, :_trd_count) for cg in cgcols], 
        
        # Number of Bonds:
        [Symbol(cg,  :_bonds) => (x -> size(unique(x[.!ismissing.(x)]), 1)) => 
            Symbol(cg, :_bonds) for cg in cgcols], 
        
        # Number of issuers:
        [Symbol(cg,  :_issuers) => (x -> size(unique(x[.!ismissing.(x)]), 1)) => 
            Symbol(cg, :_issuers) for cg in cgcols])
end

function stats_generator(df::DataFrame, combd::Dict{Symbol,Int64};
                         groupby_date_cols::Array{Symbol, 1}=[:trd_exctn_yr, :trd_exctn_qtr])
    tmpdf = df[get_filter_cond(df, combd), :]
    
    cgcols = [Symbol(:cg, x) for x in 1:15]
    fx1(dummy, var) = dummy == true ? var : 0.0
    fx2(dummy, var) = dummy == true ? var : missing
    for cg in cgcols
        tmpdf[!, Symbol(cg, :_trd_vol)] = fx1.(tmpdf[:, cg], tmpdf[:, :entrd_vol_qt])
        tmpdf[!, Symbol(cg, :_bonds)] = fx2.(tmpdf[:, cg], tmpdf[:, :cusip_id])
        tmpdf[!, Symbol(cg, :_issuers)] = fx2.(tmpdf[:, cg], tmpdf[:, :ISSUER_ID])
    end
    
    gdf = groupby(tmpdf, groupby_date_cols)

    # Number of bonds and number of trades
    df1 = combine(gdf, nrow => :total_trd_count,
                  :cusip_id => (x -> size(unique(x), 1)) => :total_bonds,
                  :ISSUER_ID => (x -> size(unique(x), 1)) => :total_issuers)

    # Volume stats
    df2 = vol_stats_generator(gdf)

    # Statistics by Covenant Categories
    df3 = cg_vol_stats_generator(gdf)

    # Join Stats DataFrames
    sdf = innerjoin(innerjoin(df1, df2, on=groupby_date_cols), df3, on=groupby_date_cols)

    # Reorder columns
    cols1 =vcat(groupby_date_cols, Symbol.(keys(combd)))
    cols2 = [x for x in Symbol.(names(sdf)) if !(x in cols1)]
    cols = vcat(cols1, cols2)
    return hcat(sdf, repeat(DataFrame(combd), inner=size(df2, 1)))[:, cols]
end
# }}}
# Storing and Retrieving the Results {{{1
function save_stats_data(dto, df::DataFrame)
    stats_data_path = string(dto.main_path, "/", dto.data_dir, "/", dto.stats_dir)
    yr_dir = string(minimum(df[:, :trd_exctn_yr]))
    if !isdir(stats_data_path)
        mkdir(stats_data_path)
    end
    if !isdir(string(stats_data_path, "/", yr_dir))
        mkdir(string(stats_data_path, "/", yr_dir))
    end

    # Get date columns identifier
    date_cols = [Symbol(x) for x in names(df) if 
                any([occursin(y, x) for y in ["yr", "qtr", "mo"]])]
    fd(x) = occursin("yr", string(x)) ? "" : occursin("qtr", string(x)) ? :Q : :m
    dateid = Symbol([Symbol(fd(x), Int64(minimum(df[:, x]))) for x in date_cols]...)
    
    # Type of Statistics
    # type 1: by number of covenants
    # type 2: by covenant category
    type2_cond = all([any([occursin(string("cg", x), y) for y in names(df)]) for x in 1:15])
    dftype =  "stats_by_num_cov"
    if type2_cond
        dftype =  "stats_by_cov_cat"
    end

    fname = string(dateid, "_", dftype, ".csv")
    println(" ")
    println("Filename: ", fname)
    println(" ")
    println("Saving dataframe to folder: ", string(stats_data_path, "/", yr_dir), "...")
    CSV.write(string(stats_data_path, "/", yr_dir, "/", fname), df)
    println("Done!")
end

function load_stats_data(dto, yr::Int64, qtr::Int64; stats_by_num_cov::Bool=true)
    stats_data_path = string(dto.main_path, "/", dto.data_dir, "/", dto.stats_dir)
    yr_dir = yr
    if !isdir(stats_data_path)
        mkdir(stats_data_path)
    end
    if !isdir(string(stats_data_path, "/", yr_dir))
        mkdir(string(stats_data_path, "/", yr_dir))
    end

    dateid = Symbol(yr, :Q, qtr)
    
    # Type of Statistics
    dftype = "stats_by_num_cov"
    if !stats_by_num_cov
        dftype = "stats_by_cov_cat"
    end

    fname = string(dateid, "_", dftype, ".csv")
    println(" ")
    println("Filename: ", fname)
    println(" ")
    println("Reading dataframe in folder: ", string(stats_data_path, "/", yr_dir), "...")
    df = DataFrame(CSV.File(string(stats_data_path, "/", yr_dir, "/", fname)))

    # Parse Columns
    columns = [x for x in [:sbm, :rt, :cvt] if x in Symbol.(names(df))]
    for col in columns
        df[!, col] = Symbol.(df[:, col])
    end

    return df
end
# }}}1
end

