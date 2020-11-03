# vim: set fdm=marker :

using DataFrames
using CSV
# using Distributed
# using ImageMagick
using FileIO
using VegaLite

main_path = "/home/artur/BondPricing/bond-data"
scripts_path = string(main_path, "/data-scripts/plots")
plt_dir = "plots"

module_path = string(main_path, "/module")
include(string(joinpath(module_path, "data_module"), ".jl"))
include(string(joinpath(module_path, "stats_module"), ".jl"))
include(string(joinpath(module_path, "plot_module"), ".jl"))

# NEED TO LOAD DFA
dto = DataMod.data_obj_constructor()

# yr and qtr are defined outside this script
scc = StatsMod.load_stats_data(dto, yr, qtr; stats_by_num_cov=false)

# scc = deepcopy(scc)

# Choose sections to be run below: {{{1
run_secs = Dict{Symbol, Bool}(:issuers => false,
                              :bonds => false,
                              :trade_count => true,
                              :trade_volume => true)
run_all = true
[run_secs[k] = true for k in keys(run_secs) if run_all]
# }}}1
# Common Parameters {{{1
color_scale="viridis"
cal_formula=""
cal_var=""
col_var="cov_cat"
col_var_type="ordinal"
col_title="Covenant Category"
col_sort="ascending"
x_var="sbm:n"
x_var_type="nominal"
x_axis_title=" "
width_step=18
legend_title="Secondary Market"
spacing=4.5
height=350
save_plt=true
plt_type = "cov_cat"
file_ext="png"
# }}}1
pl = [ ]

# Issuers per Covenant Category {{{1
if run_secs[:issuers]
    stats_var=:issuers
    tt = PlotMod.prepare_cat_plot(scc; stat=stats_var)
    rt_tt = PlotMod.get_ats_otc_diffs_by_rt(scc, stats_var)

    color_scale="viridis"
    cal_formula=""
    x_var="sbm:n"
    x_var_type="nominal"
    x_axis_title=" "
    legend_title="Secondary Market"

# Issuer Count {{{2
y_var="value"
y_axis_title="Number of Issuers"
title=[string("Number of Issuers of Non-MTN-Bonds Traded by Covenant Category")]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}2
# Issuer Percentage Count {{{2
y_var="perc_total"
y_axis_title="% of Total Number of Issuers"
title=[string("Number of Issuers by Covenant Category as a Percentage of "),
              "the Number of Issuers of Non-MTN-Bonds Traded"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}2
# Issuer Percentage Count by Secondary Market {{{2
y_var="perc_sbm_total"
y_axis_title="% of Total Number of Issuers"
title=["Number of Issuers by Covenant Category as a Percentage of the Total ",
       string("Number of Issuers of Non-MTN-Bonds ",
              "Traded by Secondary Bond Market")]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}2
# Issuer Absolute Diff by Secondary Market {{{2
tt = deepcopy(rt_tt[rt_tt[:, :sbm] .== :otc, :])

color_scale="bluepurple"

cal_formula = ""
cal_legend="Secondary Bond Market"
cal_var=:sbm
x_var="rt:n"
x_var_type="nominal"
legend_title="Rating"
#height=250

y_var="value_diff"
y_axis_title="Absolute Difference in the Number of Issuers"
title=["ATS v.s. OTC Absolute Difference in Rating-Contingent Number of Issuers of " ,
       "Non-MTN-Bonds by Covenant Category"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}2
# Issuer Percentage Diff by Secondary Market {{{2
y_var="perc_diff"
y_axis_title="% Difference in the Number of Issuers"
title=["ATS v.s. OTC % Difference in Rating-Contingent Number of Issuers of " ,
       "Non-MTN-Bonds by Covenant Categories"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}2
end
# }}}1
# Bonds per Covenant Category {{{1
if run_secs[:bonds]
    stats_var = :bonds
    tt = PlotMod.prepare_cat_plot(scc; stat=stats_var)
    rt_tt = PlotMod.get_ats_otc_diffs_by_rt(scc, stats_var)

    color_scale="viridis"
    cal_formula=""
    x_var="sbm:n"
    x_var_type="nominal"
    x_axis_title=" "
    legend_title="Secondary Market"

# Bond Count {{{2
y_var="value"
y_axis_title="Number of Bonds"
title=[string("Number of Non-MTN Bonds Traded per Covenant Category")]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}2
# Bond Percentage Count {{{2
y_var="perc_total"
y_axis_title="% of Total Non-MTN Bonds Traded"
title=["Non-MTN Bonds by Covenant Category as a Percentage of ",
       "the Total Number of Non-MTN Bonds Traded"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}2
# Bond Percentage Count by Secondary Market {{{2
y_var="perc_sbm_total"
y_axis_title="% of Total Non-MTN Bonds Traded"
title=["Non-MTN-Bonds by Covenant Category as a Percentage of the ",
       string("Total Number of Non-MTN Bonds Traded ",
              "by Secondary Bond Market")]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}2
# Bond Absolute Diff by Secondary Market {{{2
tt = deepcopy(rt_tt[rt_tt[:, :sbm] .== :otc, :])

color_scale="bluepurple"

cal_formula = ""
cal_legend="Secondary Bond Market"
cal_var=:sbm
x_var="rt:n"
x_var_type="nominal"
legend_title="Rating"
#height=250

y_var="value_diff"
y_axis_title="Absolute Difference in the Number of Bonds Traded"
title=["ATS v.s. OTC Absolute Difference in Rating-Contingent Number of " ,
       "Non-MTN Bonds Traded by Covenant Category"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}2
# Bond Percentage Diff by Secondary Market {{{2
y_var="perc_diff"
y_axis_title="% Difference in the Number of Bonds Traded"
title=["ATS v.s. OTC % Difference in Rating-Contingent Number of " ,
       "Non-MTN Bonds Traded by Covenant Category"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}2
end
# }}}1
# Trade Count per Covenant Category {{{1
if run_secs[:trade_count]
    stats_var = :count
    tt = PlotMod.prepare_cat_plot(scc; stat=stats_var)
    rt_tt = PlotMod.get_ats_otc_diffs_by_rt(scc, stats_var)

    color_scale="viridis"
    cal_formula=""
    x_var="sbm:n"
    x_var_type="nominal"
    x_axis_title=" "
    legend_title="Secondary Market"

# Trade Count {{{2
y_var="value"
y_axis_title="Number of Trades"
title=[string("Trade Count of Non-MTN Bonds by Covenant Category")]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}2
# Trade Percentage Count {{{2
y_var="perc_total"
y_axis_title="% of Total Trade Count"
title=["Non-MTN-Bond Trades by Covenant Category as Percentage of ",
       "the Total Non-MTN-Bond Trade Count"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}2
# Trade Percentage Count by Secondary Market {{{2
y_var="perc_sbm_total"
y_axis_title="% of Total Trade Count"
title=["Non-MTN-Bond Trades by Covenant Category as a Percentage of the ",
       string("Total Non-MTN-Bond Trade Count ",
              "by Secondary Bond Market")]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}2
# Trade Count Absolute Diff by Secondary Market {{{2
tt = deepcopy(rt_tt[rt_tt[:, :sbm] .== :otc, :])

color_scale="bluepurple"

cal_formula = ""
cal_legend="Secondary Bond Market"
cal_var=:sbm
x_var="rt:n"
x_var_type="nominal"
legend_title="Rating"
#height=250

y_var="value_diff"
y_axis_title="Absolute Difference in Trade Count"
title=["ATS v.s. OTC Absolute Difference in Rating-Contingent Trade Count of " ,
       "Non-MTN-Bonds by Covenant Category"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}2
# Trade Count Percentage Diff by Secondary Market {{{2
y_var="perc_diff"
y_axis_title="% Difference in Trade Count"
title=["ATS v.s. OTC % Difference in Rating-Contingent Trade Count of " ,
       "Non-MTN-Bonds by Covenant Category"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}2
end
# }}}1
# Trade Volume per Covenant Category {{{1
if run_secs[:trade_volume]
    stats_var = :volume
    tt = PlotMod.prepare_cat_plot(scc; stat=stats_var)
    rt_tt = PlotMod.get_ats_otc_diffs_by_rt(scc, stats_var)

    color_scale="viridis"
    cal_formula=""
    x_var="sbm:n"
    x_var_type="nominal"
    x_axis_title=" "
    legend_title="Secondary Market"

# Trade Volume {{{2
y_var="value"
y_axis_title="Trade Voume (USD tn)"
title=[string("Trade Volume of Non-MTN Bonds by Covenant Category")]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}2
# Trade Volume Percentage {{{2
y_var="perc_total"
y_axis_title="% of Total Trade Volume"
title=["Non-MTN-Bond Trade Volume by Covenant Category as Percentage of ",
       "the Total Non-MTN-Bond Trade Volume"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}2
# Trade Volume Percentage by Secondary Market {{{2
y_var="perc_sbm_total"
y_axis_title="% of Total Trade Volume"
title=["Non-MTN-Bond Trade Volume by Covenant Category as a Percentage of ",
       "the Total Non-MTN-Bond Trade Volume by Secondary Bond Market"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}2
# Trade Volume Absolute Diff by Secondary Market {{{2
tt = deepcopy(rt_tt[rt_tt[:, :sbm] .== :otc, :])

color_scale="bluepurple"

cal_formula = ""
cal_legend="Secondary Bond Market"
cal_var=:sbm
x_var="rt:n"
x_var_type="nominal"
legend_title="Rating"
#height=250

y_var="value_diff"
y_axis_title="Absolute Difference in Trade Volume (USD tn)"
title=["ATS v.s. OTC Absolute Difference in Rating-Contingent Trade Volume of ",
       "Non-MTN-Bonds by Covenant Category"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}2
# Trade Volume Percentage Diff by Secondary Market {{{2
y_var="perc_diff"
y_axis_title="% Difference in Trade Volume"
title=["ATS v.s. OTC % Difference in Rating-Contingent Trade Volume of ",
       "Non-MTN-Bonds by Covenant Category"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
#}}}2
end
# }}}1
