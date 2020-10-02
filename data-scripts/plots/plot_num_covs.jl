# vim: set fdm=marker :

using DataFrames
using CSV
# using Distributed
# using ImageMagick
using VegaLite 

main_path = "/home/artur/BondPricing/bond-data"
scripts_path = string(main_path, "/data-scripts/plots")
plt_dir = "plots"

module_path = string(main_path, "/module")
include(string(joinpath(module_path, "data_module"), ".jl"))
include(string(joinpath(module_path, "stats_module"), ".jl"))
include(string(joinpath(module_path, "plot_module"), ".jl"))

# Common Parameters {{{1
row_var="sum_num_cov"
row_var_type="ordinal"
row_var_title="Number of Covenant Categories per Bond"

col_var="sum_num_cov"
col_var_type="ordinal"
col_sort="ascending"
col_title="Number of Covenant Categories per Bond" 

x_var="sbm:n"
x_var_type="nominal"
x_axis_title=" "
width_step=18
legend_title="Secondary Market"
spacing=4.5
height=400
save_plt=true
plt_type = "num_cov"
file_ext="png"
# }}}
pl = [ ]

# yr and qtr are defined outside this script
snc = StatsMod.load_stats_data(dto, yr, qtr; stats_by_num_cov=true)

# Issuers {{{1
stats_var=:issuers
df = PlotMod.prepare_num_cov_plot(snc; stat=stats_var)

# Issuer Count {{{2
color_scale="viridis"

cond = .&(df[:, :sbm] .!= :any, df[:, :rt] .== :any)
tt = df[cond, :]

x_var="sbm:n"
x_var_type="nominal"
legend_title="Secondary Market"

y_var="value"
y_axis_title="Number of Issuers"
title=["Number of Issuers of Non-MTN-Bonds Traded",
        " by Number of Covenant Categories per Bond"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}
# Issuer Percentage Count {{{2
color_scale="viridis"

cond = .&(df[:, :sbm] .!= :any, df[:, :rt] .== :any)
tt = df[cond, :]

x_var="sbm:n"
x_var_type="nominal"
legend_title="Secondary Market"

y_var="perc_sbm_total"
y_axis_title="Share of Issuers"
title=["Secondary-Market-Contingent Share of Issuers of Non-MTN-Bonds Traded",
       " by Number of Covenant Categories per Bond"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}
# Issuer Percentage Count 2 {{{2
color_scale="bluepurple"

x_var="rt:n"
x_var_type="nominal"
legend_title="Rating"

y_var="perc_sbm_rt_total"
y_axis_title="Share of Issuers"

# ATS
cond = .&(df[:, :sbm] .== :ats, df[:, :rt] .!= :any)
tt = df[cond, :]

title=["ATS - Rating-Contingent Share of Issuers of Non-MTN-Bonds",
       " by Number of Covenant Categories per Bond"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)

# OTC
cond = .&(df[:, :sbm] .== :otc, df[:, :rt] .!= :any)
tt = df[cond, :]

title=["OTC - Rating-Contingent Share of Issuers of Non-MTN-Bonds",
       " by Number of Covenant Categories per Bond"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}
# ATS OTC {{{2
color_scale="bluepurple"

cond = .&(df[:, :sbm] .!= :any, df[:, :rt] .!= :any)
tt  = df[cond, :]
tt[:, "sbm2"] .= uppercase.(string.(tt[:, :sbm]))

x_var="rt:n"
x_var_type="nominal"
legend_title="Rating"
# cal_formula = string("datum.sbm2 == 'ats'  ? 'ATS' : 'OTC'")
height=250
row_var="sbm2"
spacing=5.
# row_var_type="nominal"
# row_var_title="Secondary Bond Market"

row_var_title="% of Rating- & Market-Contingent Issuers" 
y_var="perc_sbm_rt_total"
y_axis_title=""
# y_axis_title="% of Total Trade Volume by Secondary Market"
title=["Rating- and Secondary-Market-Contingent Share of Issuers Non-MTN-Bond", 
       " by Number of Covenant Categories per Bond"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "dual_vega_plt_script.jl"))
push!(pl, p)
# }}}
# Volume Percentage Diff by Secondary Market {{{2
color_scale="bluepurple"

cond = .&(df[:, :sbm] .== :ats, df[:, :rt] .!= :any)
ats_vol  = df[cond, :perc_sbm_rt_total]

cond = .&(df[:, :sbm] .== :otc, df[:, :rt] .!= :any)
otc_vol = df[cond, :perc_sbm_rt_total]
tt  = df[cond, :]
tt[!, :diff] = ats_vol - otc_vol

cal_formula = ""
cal_legend="Secondary Bond Market"
cal_var=:sbm
x_var="rt:n"
x_var_type="nominal"
legend_title="Rating"
height=250

y_var="diff"
y_axis_title="% Diff in the Number of Issuers"
title=["ATS v.s. OTC % Difference in Rating- Contingent Number of Non-MTN-Bond Issuers" ,
       " by Number of Covenant Categories per Bond"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}
# }}}
# Bonds {{{1
stats_var=:bonds
df = PlotMod.prepare_num_cov_plot(snc; stat=stats_var)

# Bond Count {{{2
color_scale="viridis"

cond = .&(df[:, :sbm] .!= :any, df[:, :rt] .== :any)
tt = df[cond, :]

x_var="sbm:n"
x_var_type="nominal"
legend_title="Secondary Market"

y_var="value"
y_axis_title="Number of Issuers"
title=["Number of Non-MTN-Bonds Traded",
        " by Number of Covenant Categories per Bond"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}
# Bond Percentage Count {{{2
color_scale="viridis"

cond = .&(df[:, :sbm] .!= :any, df[:, :rt] .== :any)
tt = df[cond, :]

x_var="sbm:n"
x_var_type="nominal"
legend_title="Secondary Market"

y_var="perc_sbm_total"
y_axis_title="Share of Issuers"
title=["Secondary-Market-Contingent Share of Non-MTN-Bonds Traded",
       " by Number of Covenant Categories per Bond"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}
# Bond Percentage Count 2 {{{2
color_scale="bluepurple"

x_var="rt:n"
x_var_type="nominal"
legend_title="Rating"

y_var="perc_sbm_rt_total"
y_axis_title="Share of Issuers"

# ATS
cond = .&(df[:, :sbm] .== :ats, df[:, :rt] .!= :any)
tt = df[cond, :]

title=["ATS - Rating-Contingent Share of Non-MTN-Bonds",
       " by Number of Covenant Categories per Bond"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)

# OTC
cond = .&(df[:, :sbm] .== :otc, df[:, :rt] .!= :any)
tt = df[cond, :]

title=["OTC - Rating-Contingent Share of Non-MTN-Bonds",
       " by Number of Covenant Categories per Bond"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}
# ATS OTC {{{2
color_scale="bluepurple"

cond = .&(df[:, :sbm] .!= :any, df[:, :rt] .!= :any)
tt  = df[cond, :]
tt[:, "sbm2"] .= uppercase.(string.(tt[:, :sbm]))

x_var="rt:n"
x_var_type="nominal"
legend_title="Rating"
# cal_formula = string("datum.sbm2 == 'ats'  ? 'ATS' : 'OTC'")
height=250
row_var="sbm2"
spacing=5.
# row_var_type="nominal"
# row_var_title="Secondary Bond Market"

row_var_title="% of Rating- & Market-Contingent Issuers" 
y_var="perc_sbm_rt_total"
y_axis_title=""
# y_axis_title="% of Total Trade Volume by Secondary Market"
title=["Rating- and Secondary-Market-Contingent Share of Non-MTN Bonds", 
       " by Number of Covenant Categories per Bond"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(joinpath(module_path, "plot_module"), ".jl"))
include(string(scripts_path, "/", "dual_vega_plt_script.jl"))
push!(pl, p)
# }}}
# Volume Percentage Diff by Secondary Market {{{2
color_scale="bluepurple"

cond = .&(df[:, :sbm] .== :ats, df[:, :rt] .!= :any)
ats_vol  = df[cond, :perc_sbm_rt_total]

cond = .&(df[:, :sbm] .== :otc, df[:, :rt] .!= :any)
otc_vol = df[cond, :perc_sbm_rt_total]
tt  = df[cond, :]
tt[!, :diff] = ats_vol - otc_vol

cal_formula = ""
cal_legend="Secondary Bond Market"
cal_var=:sbm
x_var="rt:n"
x_var_type="nominal"
legend_title="Rating"
height=250

y_var="diff"
y_axis_title="% Diff in the Number of Bonds"
title=["ATS v.s. OTC % Difference in Rating- Contingent Number of Non-MTN Bonds" ,
       " by Number of Covenant Categories per Bond"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}
# }}}
# Trade Volume {{{1
stats_var = :volume
df = PlotMod.prepare_num_cov_plot(snc; stat=stats_var)

# Total Volume {{{2
color_scale="viridis"

cond = .&(df[:, :sbm] .!= :any, df[:, :rt] .== :any)
tt  = df[cond, :]

x_var="sbm:n"
x_var_type="nominal"
legend_title="Secondary Market"

y_var = "value"
y_axis_title = "Trade Volume (USD tr)"
title=[string("Trade Volume of Non-MTN Bonds by Secondary Market")]
if :period in Symbol.(names(df))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}
# Share of Total Volume by SBM {{{2
color_scale = "viridis" 

cond = .&(df[:, :sbm] .!= :any, df[:, :rt] .== :any)
tt  = df[cond, :]

y_var="perc_sbm_total"
y_axis_title="% of Market-Contingent Trade Volume"
title=["Secondary-Market-Contingent Share of Non-MTN-Bond Trade Volume",
       " by Number of Covenant Categories per Bond"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)

# Volume Percentage Count by Secondary Market {{{2
# ATS {{{3
# Register a discrete color scheme named "basic" that can then be used in Vega specs
# cs_basic = VegaLite.scheme("basic", ["#f00", "#0f0"])
# color_scale=cs_basic
color_scale="bluepurple"

cond = .&(df[:, :sbm] .== :ats, df[:, :rt] .!= :any)
tt  = df[cond, :]

x_var="rt:n"
x_var_type="nominal"
legend_title="Rating"

y_var="perc_sbm_rt_total"
y_axis_title="% of Rating-Contingent ATS Trade Volume"
title=["ATS - Rating-Contingent Share of Total Non-MTN-Bond Trade Volume",
       " by Covenant Categories per Bond"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}
# OTC {{{3
color_scale="bluepurple"

cond = .&(df[:, :sbm] .== :otc, df[:, :rt] .!= :any)
tt  = df[cond, :]

x_var="rt:n"
x_var_type="nominal"
legend_title="Rating"

y_var="perc_sbm_rt_total"
y_axis_title="% of Rating-Contingent OTC Trade Volume"
title=["OTC - Rating-Contingent Share of Total Non-MTN-Bond Trade Volume",
       " by Covenant Categories per Bond"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}
# Volume Percentage Count by Rating & Secondary Market {{{3
color_scale="bluepurple"

cond = .&(df[:, :sbm] .!= :any, df[:, :rt] .!= :any)
tt  = df[cond, :]
tt[:, "sbm2"] .= uppercase.(string.(tt[:, :sbm]))

x_var="rt:n"
x_var_type="nominal"
legend_title="Rating"
# cal_formula = string("datum.sbm2 == 'ats'  ? 'ATS' : 'OTC'")
height=250
row_var="sbm2"
spacing=5.
# row_var_type="nominal"
# row_var_title="Secondary Bond Market"

row_var_title="% of Rating- & Market-Contingent Trade Volume" 
y_var="perc_sbm_rt_total"
y_axis_title=""
# y_axis_title="% of Total Trade Volume by Secondary Market"
title=["Rating- and Secondary-Market-Contingent Share of Total Non-MTN-Bond Trade Volume", 
       " by Number of Covenant Categories per Bond"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(joinpath(module_path, "plot_module"), ".jl"))
include(string(scripts_path, "/", "dual_vega_plt_script.jl"))
push!(pl, p)
# }}}
# }}}
# Volume Percentage Diff by Secondary Market {{{2
color_scale="bluepurple"

cond = .&(df[:, :sbm] .== :ats, df[:, :rt] .!= :any)
ats_vol  = df[cond, :perc_sbm_rt_total]

cond = .&(df[:, :sbm] .== :otc, df[:, :rt] .!= :any)
otc_vol = df[cond, :perc_sbm_rt_total]
tt  = df[cond, :]
tt[!, :diff] = ats_vol - otc_vol

cal_formula = ""
cal_legend="Secondary Bond Market"
cal_var=:sbm
x_var="rt:n"
x_var_type="nominal"
legend_title="Rating"
height=250

y_var="diff"
y_axis_title="% Trade Volume Diff by Secondary Market"
title=["ATS v.s. OTC % Difference in Rating- Contingent Non-MTN Bond Trade Volume" ,
       " by Number of Covenant Categories per Bond"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}
# }}}
