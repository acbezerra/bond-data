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
stats_var=:issuers
tt = PlotMod.prepare_cat_plot(scc; stat=stats_var)
ig_tt = PlotMod.prepare_cat_plot(scc; stat=stats_var, rt=:ig)
hy_tt = PlotMod.prepare_cat_plot(scc; stat=stats_var, rt=:hy)
rt_tt = vcat(ig_tt, hy_tt)


## Issuer Count {{{2
y_var="value"
y_axis_title="Number of Issuers"
title=[string("Number of Issuers of Non-MTN-Bonds Traded per Covenant Category")]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}2
## Issuer Percentage Count {{{2
y_var="perc_total"
y_axis_title="% of Total Number of Issuers"
title=[string("Number of Issuers by Covenant Category as Percentage of "),
              "Number of Issuers of Non-MTN-Bonds Traded"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}2
## Issuer Percentage Count by Secondary Market {{{2
y_var="perc_sbm_total"
y_axis_title="% of Total Non-MTN Bonds Traded by Secondary Market"
title=["Number of Issuers by Covenant Category as Percentage of Total",
              string("Number of Issuers of Non-MTN-Bonds ",
                     "Traded by Secondary Bond Market")]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}
# Bonds per Covenant Category {{{1
stats_var = :bonds
tt = PlotMod.prepare_cat_plot(scc; stat=stats_var)

## Bond Count
y_var="value"
y_axis_title="Number of Bonds"
title=[string("Number of Non-MTN Bonds Traded per Covenant Category")]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)

## Bond Percentage Count
y_var="perc_total"
y_axis_title="% of Total Non-MTN Bonds Traded"
title=[string("Non-MTN-Bond Trades by Covenant Category as Percentage of Total ",
              "Non-MTN Bonds Traded")]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end


include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)

## Bond Percentage Count by Secondary Market
y_var="perc_sbm_total"
y_axis_title="% of Total Non-MTN Bonds Traded by Secondary Market"
title=[string("Non-MTN-Bond Trades by Covenant Category as Percentage of Total ",
              "Non-MTN Bonds Traded"),
       "by Secondary Bond Market"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}
# Trade Count per Covenant Category {{{1
stats_var = :count
tt = PlotMod.prepare_cat_plot(scc; stat=stats_var)

## Trade Count
y_var="value"
y_axis_title="Number of Trades"
title=[string("Trade Count of Non-MTN Bonds by Covenant Category")]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)

## Trade Percentage Count
y_var="perc_total"
y_axis_title="% of Total Trade Count"
title=[string("Non-MTN-Bond Trades by Covenant Category as Percentage of Total ",
              "Non-MTN-Bond Trade Count")]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)

## Trade Percentage Count by Secondary Market
y_var="perc_sbm_total"
y_axis_title="% of Total Trade Count by Secondary Market"
title=[string("Non-MTN-Bond Trades by Covenant Category as Percentage of Total ",
              "Non-MTN-Bond Trade Count"),
       "by Secondary Bond Market"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}
# Volume per Covenant Category {{{1
stats_var = :volume
tt = PlotMod.prepare_cat_plot(scc; stat=stats_var)

## Volume
y_var="value"
y_axis_title="Trade Voume (USD tn)"
title=[string("Trade Volume of Non-MTN Bonds by Covenant Category")]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)

## Volume Percentage Count
y_var="perc_total"
y_axis_title="% of Total Trade Volume"
title=[string("Non-MTN-Bond Trade Volume by Covenant Category as Percentage of "),
              "Total Non-MTN-Bond Trade Volume"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)

## Volume Percentage Count by Secondary Market
y_var="perc_sbm_total"
y_axis_title="% of Total Trade Volume by Secondary Market"
title=["Non-MTN-Bond Trades by Covenant Category as Percentage of Total ",
       "Non-MTN-Bond Trade Count by Secondary Bond Market"]
if :period in Symbol.(names(tt))
    title[end] = string(title[end], " - ", tt[1, :period])
end

include(string(scripts_path, "/", "single_vega_plt_script.jl"))
push!(pl, p)
# }}}
