# %%
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

ENV["LINES"] = 100
ENV["COLUMNS"] = 1000

# %%
# using WebIO
# WebIO.install_jupyter_serverextension()
# WebIO.install_jupyter_nbextension()

include(string(joinpath(module_path, "data_module"), ".jl"))
dto = DataMod.data_obj_constructor()
# %%
# mdf = @time DataMod.get_mergent_fisd_df(dto;
#                                         rmnp=rmnp,
#                                         min_maturity=min_maturity,
#                                         filter_rating=filter_rating,
#                                         rating_types=rating_types,
#                                         bond_id=bond_id,
#                                         eval_merge=eval_merge,
#                                         miss_date=miss_date)
@time include(string(script_path, "/mergent_filter.jl"))
first(mdf, 5)


# %% Step 1 - Get MERGENT Filtered Dataset #####################################
include(string(joinpath(module_path, "data_module"), ".jl"))
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
    mdf = @time DataMod.load_mergent_filtered_df(dto)
else
    # load original files, merge and filter:
    include(string(script_path, "/mergent_filter.jl"))
end
cols = [:COMPLETE_CUSIP, :RATING, :rating_date, :RATING_TYPE]
first(mdf[:, cols], 5)
# ##############################################################################

# %%
mdf2 = @time DataMod.load_mergent_filtered_df(dto; drop_cols=true)


# %% NOTES
# Remove DEFEASED and DEFEASED_DATE ?
# Investigate LEVERAGE_TEST_SUB
# COMPARE IG Variables
# Create stats by ISSUER
# % of bonds with a covenant that trade in ATS v.s. otc
# % bonds without covenant that trade in ats vs otc
# differences in the types of issuers?
# Pass stats analysis below to a separate script
# ISSUANCE date is the EFFECTIVE_DATE variable
# Date variables in Date format should be suffixed with "_date":
# 1. rating_date
# 2. issuance_date
# 3. maturity_date
# 4. trd_exctn_date

# A Note on selecting the last rating date when merging with TRACE data:
# Since values get repeated everytime one of the columns is updated,
# by getting always the last rating date entry, I am also getting the
# most recent values of the other variables.

# %%
include(string(joinpath(module_path, "data_module"), ".jl"))
@time include(string(script_path, "/merge_trace_mergent.jl"))
first(fdf, 5)

# %%
include(string(joinpath(module_path, "data_module"), ".jl"))
# Form combinations of ATS., IG and COVENANT filters
combdf =  DataMod.get_filter_combinations()
# Select cols and create smk indicator variable:
ffdf = DataMod.filter_selected(fdf)
# Generate Trade Execution Quarter variable:
ffdf[!, :trd_exctn_qtr] .= Int64.(ceil.(ffdf[!, :trd_exctn_mo]/3))

# Compute Statistics
function gen_sbm_rt_cvt_cat_vars(df::DataFrame)
    sbmf(x, y) = (x == y == 1) ? :both : (x == y) ? :any : (x == 1) ? :ats : :otc
    covenantf(x, y) = (x == y) ? :any : (x == 1) ? :cov : :ncov
    ratingf(x, y) = (x == y) ? :any : (x == 1) ? :ig : :hy

    df[!, :sbm] .= sbmf.(df[:, :ats], df[:, :otc])
    df[!, :rt] .= ratingf.(df[:, :ig], df[:, :hy])
    df[!, :cvt] .= covenantf.(df[:, :cov], df[:, :ncov])
    df[!, :rt_cvt] .= Symbol.(:rt_, df[!, :rt], :_cvt_, df[:cvt])

    return df
end

groupbycols = [:trd_exctn_yr, :trd_exctn_qtr]
dfl = @time fetch(@spawn [DataMod.stats_generator(ffdf,
                                                  DataMod.dfrow2dict(combdf, row);
                                                  groupbycols=groupbycols)
                          for row in 1:size(combdf, 1)])
dfa = sort(vcat(dfl...), groupbycols)
dfa = gen_sbm_rt_cvt_cat_vars(dfa)

# %%
zcol = :cg7_trd_vol_tr
cols = [:sbm, :rt_cvt, zcol]
tab1 = unstack(tmp[:, cols], :rt_cvt, zcol)


# %%
using Seaborn

# %%
names(tmp)

# %%
cols = [:sbm, :rt, :cvt, :total_trd_vol_tr, :cg1_trd_vol_tr]
tmp[:, cols]

# %%
using VegaLite, VegaDatasets

# %%
tmp[:, cols] |>
@vlplot(
    :bar,
    # transform=[
    #     {filter="datum.year == 2000"},
    #     {calculate="datum.sex == 2 ? 'Female' : 'Male'", as="gender"}
    # ],
    column="rt:n",
    y={"total_trd_vol_tr", axis={title="Trade Volume (USD tr)", grid=false}},
    x={"sbm:n", axis={title=""}},
    color={"gender:n", scale={range=["#675193", "#ca8861"]}},
    spacing=10,
    config={
        view={stroke=:transparent},
        axis={domainWidth=1}
    }
)

# %%
dataset("population")
# %%
dataset("population") |>
@vlplot(
    :bar,
    transform=[
        {filter="datum.year == 2000"},
        {calculate="datum.sex == 2 ? 'Female' : 'Male'", as="gender"}
    ],
    column="age:o",
    y={"sum(people)", axis={title="population", grid=false}},
    x={"gender:n", axis={title=""}},
    color={"gender:n", scale={range=["#675193", "#ca8861"]}},
    spacing=10,
    config={
        view={stroke=:transparent},
        axis={domainWidth=1}
    }
)

# %%
dataset("seattle-weather") |>
@vlplot(
    :bar,
    x={"month(date):o", axis={title="Month of the year"}},
    y="count()",
    color={
        :weather,
        scale={
            domain=["sun","fog","drizzle","rain","snow"],
            range=["#e7ba52","#c7c7c7","#aec7e8","#1f77b4","#9467bd"]
        },
        legend={
            title="Weather type"
        }
    }
)

# %%
g = Seaborn.countplot
(x=:sbm, y=:total_trd_vol, hue=:rt, data=tmp[:, cols])


# %%
.catplot(x="class", y="survived", hue="sex", data=titanic,
                height=6, kind="bar", palette="muted")


# %%
zcol = :total_trd_vol_tr
cols = [:sbm, :rt_cvt, zcol]
tab2 = unstack(tmp[:, cols], :rt_cvt, zcol)

# tab1[:, 2:end] ./
tab2[:, 2:end]




# %%
include(string(joinpath(module_path, "data_module"), ".jl"))
tmp = DataMod.stats_generator(ffdf, DataMod.dfrow2dict(combdf, 1))

# %%
Int64(ceil(7/3))

# %%


# %%
function gen_qtr(x::Int64)
    if x in [1, 2, 3]
        return 1


end

# %%
unique(ffdf[!, :trd_exctn_qtr])

# %%
ffdf[!, :trd_exctn_qtr] .= Int64.(ceil.(ffdf[!, :trd_exctn_mo]/3))



# %%
DataMod.dfrow2dict(combdf, 10)
# %%
ftdf[1, :otc]
# %%
sbmv = :ats
rating = :n
covenant = :n
yr = 2019
mo = 4
ft = DataMod.filter_constructor(sbmv, rating, covenant)
ftdf = hcat(DataFrame(:trd_exctn_yr => yr, :trd_exctn_mo => mo), DataMod.get_filter_comb(ft.sbm, ft.rating, ft.covenant))

cond = .&([abs.(dfa[:, col] .- ftdf[1, col]) .< 1e-5 for col in names(ftdf)]...)
dfa[cond, :]


# %%
dfa[dfa[:, :trd_exctn_mo] .== 4, :]

# %%
gd = groupby(fdf, [:trd_exctn_yr, :trd_exctn_mo])
combine(x -> DataMod.smk_rt_cov_indicators(x), gd)

# %%
using Statistics

# %%
mo = 4
yr = 2019
cond = .&((dfa[:, :trd_exctn_yr] .- yr) .< 1e-5,  (dfa[:, :trd_exctn_mo] .- mo) .< 1e-5)
# cond = .&(cond, dfa[:, :ig] .!= dfa[:, :hy])#, dfa[:, :cov] .!= dfa[:, :ncov])
tmp= dfa[cond, :]

# # %%
# size(unique(ffdf[.&(ffdf[:, :ig], ffdf[:, :trd_exctn_mo] .== 4), :cusip_id]), 1)
#
# # %%
# tmp[:, [:ats, :otc,	:ig, :hy, :cov, :ncov, :cg15_trd_vol_tr]]

# %%
function gen_sbm_rt_cov_cat_vars(df::DataFrame)
    sbmf(x, y) = (x == y == 1) ? :both : (x == y) ? :any : (x == 1) ? :ats : :otc
    covenantf(x, y) = (x == y) ? :any : (x == 1) ? :cov : :ncov
    ratingf(x, y) = (x == y) ? :any : (x == 1) ? :ig : :hy

    df[!, :sbm] .= sbmf.(df[:, :ats], df[:, :otc])
    df[!, :rt] .= ratingf.(df[:, :ig], df[:, :hy])
    df[!, :cvt] .= covenantf.(df[:, :cov], df[:, :ncov])
    df[!, :rt_cov] .= Symbol.(:rt_, df[!, :rt], :_cov_, df[:cvt])

    return df
end

# %%
fff(0, 0)

# %%
# covf(z) = z == 1 ? :cov : :ncov
igf(z) = z == 1 ? :ig : :hy
function sbm(a, o)
    if a == o
        return a == 1 ? :both : :any
    elseif a == 1
        return :ats
    end

    return :otc
end

function covf(a, o)
    if a == o
        return :any
    elseif a == 1
        return :cov
    end

    return :ncov
end
function ratf(a, o)
    if a == o
        return :any
    elseif a == 1
        return :ig
    end

    return :hy
end


tmp[!, :rat] .= ratf.(tmp[:, :ig], tmp[:, :hy])
tmp[!, :sbm] .= sbm.(tmp[:, :ats], tmp[:, :otc])
tmp[!, :cvt] .= covf.(tmp[:, :cov], tmp[:, :ncov])
tmp[!, :rat_cov] .= Symbol.(:rat_, tmp[!, :rat], :_cov_, tmp[:cvt])
# cols = [:ats, :otc, :sbm, :ig, :hy, :rat, :cov, :ncov, :cvt, :trd_count]
cols = [:sbm, :rat_cov, :cg15_trd_vol_tr]
tmp = sort(tmp, [:rat, :sbm, :cvt])



# %%
tab1

# %%



# %%

dd = Dict{Symbol,Int64}(:otc  => 0,
                        :hy   => 0,
                        :ig   => 0,
                        :ats  => 0,
                        :cov  => 0,
                        :ncov => 0)

DataMod.stats_generator(ffdf, dd)

# %%
cond = .&(ffdf[:, :trd_exctn_yr] .== 2019, ffdf[:, :trd_exctn_mo] .== 4, ffdf[:, :cg1])
count(cond)

# %%
sum(ffdf[cond, :entrd_vol_qt])/1e9

# %%
sum(ffdf[cond, :entrd_vol_qt])/1e9

# %%
size(ffdf)

# %%
count(DataMod.get_filter_cond(ffdf, dd))

# %%
cond = ffdf[:, :trd_exctn_mo] .== 4
sum(.*(ffdf[cond, :cg15], ffdf[cond, :entrd_vol_qt]))/1e9

# %%
sum(ffdf[.&(ffdf[:, :trd_exctn_mo] .== 4, ffdf[:, :cg15] .== 1), :entrd_vol_qt])/1e9

# %%
size(unique(ffdf[.&(ffdf[:, :ig], ffdf[:, :trd_exctn_mo] .== 4, ffdf[:, :cg15] .== 1), :cusip_id]), 1)

# %%
sum(ffdf[.&(ffdf[:, :ig] .== 0, ffdf[:, :trd_exctn_mo] .== 4, ffdf[:, :cg15] .== 1), :entrd_vol_qt])/1e9

# %%
sum(ffdf[.&(ffdf[:, :ig] .== 0, ffdf[:, :trd_exctn_mo] .== 4, ffdf[:, :cg15] .== 1, ffdf[:, :ats]), :entrd_vol_qt])/1e9

# %%
sum(ffdf[.&(ffdf[:, :ig] .== false, abs.(ffdf[:, :trd_exctn_mo] .- 4) .< 1e-5, ffdf[:, :ats],
            ffdf[:, :cg15] .== true, ffdf[:, :covenant]), :entrd_vol_qt])/1e9

# %%
sum(ffdf[.&(ffdf[:, :ig] .== false, abs.(ffdf[:, :trd_exctn_mo] .- 4) .< 1e-5,
            ffdf[:, :cg15] .== true, ffdf[:, :covenant]), :entrd_vol_qt])/1e9


# %%
gdf

# %%
cols = [:rat, :sbm_cvt, :cusips]
unstack(tmp[:, cols], :sbm_cvt, :cusips)

# %%
cols = [:rat, :sbm_cvt, :median_trd_vol]
unstack(tmp[:, cols], :sbm_cvt, :median_trd_vol)

# %%
cols = [:rat, :sbm_cvt, :total_trd_vol]
unstack(tmp[:, cols], :sbm_cvt, :total_trd_vol)


#
# The overwhelming majority of the HY bond trade volume on ats is of bonds with some sort of covenant.

# %%
190351 / 517697

# %%
59447 / 151807

# %%
4445 / 54204

# %%
tmp[:, cols]

# %%
repeat(DataFrame(comb), inner=size(df2, 1))

# %%
include(string(joinpath(module_path, "data_module"), ".jl"))
cond = .&((fdf[:, :trd_exctn_yr] .- 2019) .< 1e-5,
          (fdf[:, :trd_exctn_mo] .- 4) .< 1e-5)
dft = DataMod.smk_rt_cov_indicators(fdf[cond, :])



# %%
cols1 =vcat([:trd_exctn_yr, :trd_exctn_mo], Symbol.(names(comb)))
cols2 = [x for x in Symbol.(names(df2)) if !(x in cols1)]
cols = vcat(cols1, cols2)
hcat(df2, repeat(DataFrame(comb), inner=size(df2, 1)))[:, cols]

# %%
function return_df_col(df::DataFrame, col::Symbol)
    if col == :otc
        return .!df[:, :ats]
    elseif col == :hy
        return .!df[:, :ig]
    elseif col == :cov
        return df[:, :covenant]
    elseif col == :ncov
        return .!df[:, :covenant]
    elseif col == :vol
        return df[:, :entrd_vol_qt]
    end

    return df[:, col]
end

sbmg = [:ats, :otc]
rtg = [:ig, :hy]
covg = vcat([:cov, :ncov], [Symbol(:cg, x) for x in 1:15])
g1 = vcat([(x, y, :vol) for x in sbmg, y in rtg]...)
g2 = vcat([(x, y, :vol) for x in sbmg, y in covg]...)
g3 = vcat([(x, y, :vol) for x in rtg, y in covg]...)
g4 = vcat([(x, y, z, :vol) for x in sbmg, y in rtg, z in covg]...)
combs = vcat(g1, g2, g3, g4)

# %%
cond = .&((fdf[:, :trd_exctn_yr] .- 2019) .< 1e-5,
          (fdf[:, :trd_exctn_mo] .- 4) .< 1e-5)
tmp = fdf[cond, :]


# %%
# Variables to be dropped:
drop_varsl = [:INVESTMENT_GRADE]

# %% TRADE SIZE CATEGORIES
vol_cols = [Symbol(x) for x in names(fdf) if occursin("volg", x)]
first(fdf[:, vcat(:cusip_id, :trd_exctn_dt, :trd_exctn_tm,
                  vol_cols...)], 5)

# %%
count(mdf[:, :RULE_144A])

# %%
count(mdf[:, :PAY_IN_KIND])

# %%
gd = groupby(df[:, cols],  groups)
    cdf = combine(gd, :entrd_vol_qt =>  (x -> Statistics.quantile!(x, .25)) => :qt25_trd_vol,
                      :entrd_vol_qt => mean => :mean_trd_vol,
                      :entrd_vol_qt => median => :median_trd_vol,
                      :entrd_vol_qt => (x -> Statistics.quantile!(x, .75)) => :qt75_trd_vol,
                      :entrd_vol_qt => (x -> sum(x)/ 10^9) => :total_vol_tr,
                      nrow => :trd_count, :cusip_id => (x -> size(unique(x), 1)) => :cusips)

# %%
include(string(joinpath(module_path, "DataMod"), ".jl"))
dto = DataMod.trace_obj_constructor()
combdf = DataMod.get_smk_rt_combinations()

fdf[!, :ats_ind] = fdf[:, :ats_indicator] .!== missing
groups = [:trd_exctn_yr, :trd_exctn_mo] #, :ISSUER_ID]
cols = vcat(groups, [:ats_ind, :ig, :cusip_id, :entrd_vol_qt])
gd = groupby(fdf[:, cols],  groups)

cdf = DataFrame([])
for r in eachrow(combdf)
    cdf = DataMod.combine_smk_rt_cusips(cdf, gd, groups;
                                         by_smk=r.by_smk, smk=r.smk,
                                         by_rt=r.by_rt, rt=r.rt)
end
cdf


# %%
groups = [:trd_exctn_yr, :trd_exctn_mo, :ISSUER_ID, :cusip_id]
cols = vcat(groups, [:ats_ind, :cov_ind, :ig, :entrd_vol_qt])
gd = groupby(fdf[:, cols],  groups)

# %%
fdf[!, :ats_ind] = fdf[:, :ats_indicator] .!== missing
fdf[!, :cov_ind] .= .&(fdf[:, :COVENANTS] .!== missing, fdf[:, :COVENANTS] .== "Y")
# rename!(fdf, :ig => :ig_ind)

# Extract cols
groups = [:trd_exctn_yr, :trd_exctn_mo, :ISSUER_ID, :cusip_id]
cols = vcat(groups, [:ats_ind, :cov_ind, :ig_ind, :entrd_vol_qt])
adf = fdf[:, cols]
gd = groupby(adf,  groups)

# Compute ATS/OTC, IG/HY, COVENANT indicators
df1 = combine(x -> smk_rt_cov(x), gd)


# Although covenant is fixed, ATS/OTC and IG/HY indicators can
# change from one transaction to another.
# %% instead of
df2 = leftjoin(adf, df1, on=groups)

#

# %%
function tmpf(df, groups)
    cols = [x for x in Symbol.(names(df)) if !(x in vcat(groups, :entrd_vol_qt))]

    for col in cols
        df[!, Symbol(col, :_vol)] .= df[:, :entrd_vol_qt] .* df[:, col]
    end

    # Combine by yr, mo and issuer
    gdf = groupby(df, [:trd_exctn_yr, :trd_exctn_mo, :ISSUER_ID])

    # For each Boolean, count cusips

    # For each Volume col, compute sum
    cols2 = [x for x in Symbol.(names(df)) if occusin("vol", string(x))]
    df2 = combine(gdf, [])




end


# %%
# Now group by year and month to compute cusip stats
yrmog = groupby(df, [:trd_exctn_yr, :trd_exctn_mo])

# %% Compute stats
stats_vars = [x for x in Symbol.(names(df)) if !(x in groups)]
sdf = combine(yrmog, :cusip_id => (x -> size(unique(x), 1)) => :cusips,
              # [y => sum => Symbol(y, :_cusips) for y in stats_vars],
              [AsTable([y, :entrd_vol_qt]) => (x -> mean(x[x[:, y] .== 1, :entrd_vol_qt])) => Symbol(y, :_vol) for y in stats_vars])

# %%
stats_vars = [x for x in Symbol.(names(df)) if !(x in groups)]
sdf = combine(yrmog, :cusip_id => (x -> size(unique(x), 1)) => :cusips,
              [y => sum => Symbol(y, :_cusips) for y in stats_vars])



# %%
gdf2 = groupby(cdf, [:trd_exctn_yr, :trd_exctn_mo])
# %%
using Statistics
cdf2 = combine(gdf2, :cusips_ats => sum, :cusips_otc => sum, :cusips_both => sum,)

# %%
# cusip_ats(x) = size(, 1)
# cdf = combine(gd, AsTable([:ats_ind, :cusip_id]) => (x -> cusip_ats(x))) # => :cusips_ats)
cdf = combine(x -> cusips_ats_only(x, 1),
              x -> cusips_ats_only(x, 2),
 gd)
# rename!(cdf, [:x1 => :cusip_ats])



# %% BY SECONDARY MARKET & RATING
# Create stats by year/month/ats/ig indicator
cdf = @time stats_calculator(df, groups)

   # By SECONDARY MARKET
   # Create stats by year/month/ats indicator
   g1 = [x for x in groups if x != :ig]
   tmp = stats_calculator(df, g1; var_suffix=:_sm)
   cdf = leftjoin(cdf, tmp, on = g1)
   cdf[!, :total_vol_perc_sm] = (cdf[!, :total_vol_tr] ./ cdf[!, :total_vol_tr_sm]) .* 100
   cdf[!, :trd_count_perc_sm] = (cdf[!, :trd_count] ./ cdf[!, :trd_count_sm]) .* 100


# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

# %%
# mdf[!, :TMP_CUSIP] .= string.(mdf[!, :ISSUER_CUSIP], mdf[!, :ISSUE_CUSIP])
# count(mdf[!, :TMP_CUSIP] != mdf[!, :COMPLETE_CUSIP])

# %% ATS v.s. OTC
include(string(joinpath(module_path, "DataMod"), ".jl"))
cdf = @time DataMod.trace_stats(fdf)


# %%
fpath = string(dto.main_path, "/", dto.data_dir, "/", dto.merged_dir)
stats_fname = string("merge_stats_", yr, "_Q", qtr, ".csv")
CSV.write(string(fpath, "/", stats_fname), cdf)



# %%
tmp = @time find_multiple_occurrences(mdf, [:COMPLETE_CUSIP, :rating_date])

# %%
my_fun(x) = maximum(x[x[:, :trd_exctn_date] .>= x[:, :rating_date], :rating_date])
df4 = @linq df1 |>
            groupby(:cusip_id) |>
            leftjoin(df1, df2, on = :cusip_id => :COMPLETE_CUSIP) |>
            transform(rating_date = (x -> df2[df2[:, :COMPLETE_CUSIP] .== x[:, :cusip_id], :rating_date])) |>
            by([:cusip_id, :trd_exctn_dt], [:trd_exctn_date, :rating_date] => ((t, r) -> maximum(r[t .>= r])) => :last_rating_date) |>
            where(:rating_date .== :last_rating_date)

# %%
df3[:, :trd_exctn_dt] = convert.(Int, df3[!, :trd_exctn_dt])
# df3[:, [:cusip_id, :trd_exctn_dt, :RATING_DATE]]

# %%
using DataFramesMeta


df3 = @linq df1 |>
           leftjoin(df1, df2, on = :cusip_id => :COMPLETE_CUSIP) |>
           where(:trd_exctn_dt .> :RATING_DATE)


# %% Ratings
rcols = [:COMPLETE_CUSIP, :RATING_TYPE, :RATING_DATE, :RATING, :MATURITY]



# %%
# let's check the bonds with the highest number of changes
# count unique values per COLUMN
tmp = sort(combine(groupby(irdf[:, cols], :COMPLETE_CUSIP), nrow), :nrow; rev = true)
first(tmp, 5)

# %%
# let's identify the variables with the greater number of changes for a given cusip
cond = irdf[:, :COMPLETE_CUSIP] .== tmp[1, :COMPLETE_CUSIP] # pick the cusip with the highest number of changes
tmp2 = combine(groupby(irdf[cond, :], :COMPLETE_CUSIP), nrow, [col => (x -> size(unique(x), 1)) => Symbol(col, :_count) for col in names(irdf)])




# %%
tr_cusips = unique(fdf[:, :cusip_id])
m_cusips = unique(irdf[:, :COMPLETE_CUSIP])
tmm_cusips = intersect(tr_cusips, m_cusips)

# %%
(size(tmm_cusips, 1) / size(tr_cusips, 1)) * 100

# %%
# CUSIPS in Bond Issues DF and not in Ratings DF
tmm_cusips = setdiff(idf[:, :COMPLETE_CUSIP], rdf[:, :COMPLETE_CUSIP])

# CUSIPS in Ratings DF and not in Bond Issues DF
rmi_cusips = setdiff(rdf[:, :COMPLETE_CUSIP], idf[:, :COMPLETE_CUSIP])

# Find index of rows in Ratings DF of CUSIPS not in Bond Issues DF
rmi_index = findall(in(rmi_cusips), rdf.COMPLETE_CUSIP)

# Check that left join was succesfull:
target = size(imr_cusips, 1) + (size(rdf, 1) - size(rmi_index, 1))


# %%
[x for x in names(irdf) if occursin("ID", x)]


# %%
findall(in([tcusip]), irdf.COMPLETE_CUSIP)


# %%
fdf_fpath = string(dto.trace_path, "/", dto.proc_dir)
fdf_fname =string(dto.proc_file_prefix, "_", yr, "_Q", qtr, ".csv")

# %% Load TRACE Master file
fpath = string(dto.trace_path, "/", dto.raw_files_dir)
fname =string(dto.trace_file_prefix, "_corp_master", ".csv")
mdf = @time DataFrame!(CSV.File(string(fpath, "/", fname)))

# Keep only Corporate securities
mdf = @time mdf[mdf[:, :sub_prdct_type] .== "CORP", :]

# Drop Missing CUSIP obs
mdf = @time mdf[mdf[:, :cusip_id] .!== missing, :]
first(mdf, 5)

# %% Load Trace Filtered DataFrame
yr = 2019
qtr = 1
filter_fpath = string(dto.trace_path, "/", dto.filter_dir)
filter_fname =string(dto.filter_file_prefix, "_", yr, "_Q", qtr, ".csv")

fdf = @time DataFrame!(CSV.File(string(filter_fpath, "/", filter_fname), types=dto.colsd))
first(fdf, 5)


# %% Merge Filtered and Master DataFrames
fcusips = unique(fdf[:, :cusip_id])
mcusips = unique(mdf[:, :cusip_id])
missing_cusips = @time [x for x in fcusips if !(x in mcusips)]
# matches = @time in(fcusips).(mcusips)    # slow

# %%
dto.trace_file_prefix

# %%
using StatsBase

pdf[!, :trd_exctn_date] .= Date.(pdf[:, :trd_exctn_dt], dateformat"yyyymmdd")
pdf[!, :stlmnt_date] .= Date.(string.(pdf[:, :stlmnt_dt]), dateformat"yyyymmdd")
dd = StatsBase.countmap(pdf[!, :stlmnt_date] .- pdf[!, :trd_exctn_date])
ddf = DataFrame(dd)
ddf[!, :id] = 1:size(ddf, 1)
ddf2 = stack(ddf, names(ddf))
ddf2[!, :perc_val] .= (ddf2[:, :value] ./sum(ddf2[:, :value])) .* 100
ddf2[!, :cum_sum] .= cumsum(ddf2[!, :perc_val])
ddf2



# %%
describe(pdf; cols=["entrd_vol_qt", "ats_indicator"])

# %%
# categorical!(pdf, [:cntra_mp_id, :rpt_side_cd])

# Group-By cols
gb_cols = [:cntra_mp_id, :rpt_side_cd]

gdf = combine(groupby(pdf[:, gb_cols], gb_cols), :rpt_side_cd=>length)
tdf = unstack(gdf, :cntra_mp_id, :rpt_side_cd, :rpt_side_cd_length)
tdf[!, :total] = tdf[!, :B] .+ tdf[!, :S]
select!(tdf, :, [:B, :S] .=> (x -> (x./tdf[:, :total]) .* 1e2) .=> [:B_row_perc, :S_row_perc],
               [:B, :S] .=> (x -> (x./sum(x)) .* 1e2) .=> [:B_col_perc, :S_col_perc] )



# %%
dto.fil


# %%
alt_cols = ["trd_exctn_yr", "trd_exctn_mo", "entrd_vol_qt", "ats_indicator", "rpt_side_cpcty"]
gd = groupby(pdf[:, alt_cols],  ["trd_exctn_yr", "trd_exctn_mo", "ats_indicator"])



# %%
gd2 = @time groupby(pdf[:, alt_cols],  ["trd_exctn_yr", "trd_exctn_mo", "ats_indicator", "rpt_side_cpcty"])

# %%
using Statistics

cgdf2 = @time combine(gd2, :entrd_vol_qt => (x -> sum(x)) => :sum),
                           :entrd_vol_qt => x -> Statistics.quantile!(x, .25) => :q25),
                           :entrd_vol_qt => median,
                           :entrd_vol_qt => q75, nrow)
