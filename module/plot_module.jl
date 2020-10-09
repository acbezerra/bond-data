# vim: set fdm=marker :

module PlotMod

using DataFrames
using VegaLite
using CSV

# Auxiliary Functions {{{1
function get_period(df::DataFrame, date_cols::Array{Symbol, 1})
    yr = :trd_exctn_yr in date_cols ? unique(df[:, :trd_exctn_yr])[1] : missing
    qtr = :trd_exctn_qtr in date_cols ? unique(df[:, :trd_exctn_qtr])[1] : missing
    mo = :trd_exctn_mo in date_cols ? unique(df[:, :trd_exctn_mo])[1] : missing
    period = (yr !== missing) ? Symbol(yr) : Symbol("")
    period = (qtr !== missing) ? Symbol(period, :Q, qtr) : period
    period = (mo !== missing) ? Symbol(period, :M, mo) : period
    
    return period
end


# function get_filter_cond(sbm::Symbol, rt::Symbol, cvt::Symbol)
#     if !(sbm in [:any, :ats, :otc, :both])
#         println("Error! sbm value must be :any, :ats, :otc or :both. Exiting...")
#         return
#     elseif !(rt in [:any, :ig, :hy])
#         println("Error! rt value must be :any, :ig or :hy. Exiting...")
#         return
#     elseif !(cvt in [:any, :cov, :ncov])
#         println("Error! rt value must be :any, :ig or :hy. Exiting...")
#         return
#     end

#     return .&(df[:, :rt] .== rt, df[:, :cvt] .== cvt)
# end

function get_plt_fpath_name(plt_type::String, stats_var::Symbol, 
                            y_var::String, period::String, 
                            main_path::String, 
                            plt_dir::String;
                            dual_plt::Bool=false,
                            file_ext::String="eps")

    plots_main_dir_path = string(main_path, "/", plt_dir)
    if !isdir(plots_main_dir_path)
        mkdir(plots_main_dir_path)
    end
    plots_dir_path = string(plots_main_dir_path, "/", period)
    if !isdir(plots_dir_path)
        mkdir(plots_dir_path)
    end
    

    dual_name = dual_plt ? "_dual" : "" 

    fname = string(period, "_", plt_type, "_", stats_var, "_", y_var, dual_name)

    return string(plots_dir_path, "/", fname, ".", file_ext)
end
# }}}
# DataFrame Manipulation Functions {{{1
# function prepare_sbm_rt_cov_vol_df(dfa::DataFrame, catn::Int64; 
#                                    date_cols::Array{Symbol, 1}=Symbol[])
#     cat_var = Symbol(:cg, catn, :_trd_vol_tr)

#     cols = [:sbm, :rt, :cvt, :total_trd_vol_tr, cat_var]
#     cond = .&(dfa[:, :sbm] .!= :any, dfa[:, :sbm] .!= :both, 
#               dfa[:, :rt] .!= :any,
#               dfa[:, :cvt] .== :cov)
#     df = dfa[cond, cols]
#     df[:, :perc_trd_vol] .= (df[:, cat_var] ./ df[:, :total_trd_vol_tr]) * 1e2

#     # Rename Rating and Secondary Bond Market Categories:
#     rt_renamer(x) = x == :ig ? "Investment Grade" : "High Yield"
#     sbm_renamer(x) = uppercase(string(x))
#     df[!, :RT] = rt_renamer.(df[:, :rt])
#     df[!, :SBM] = sbm_renamer.(df[:, :sbm])
    
#     if !isempty(date_cols)
#         df[!, :period]  .= get_period(dfa, date_cols)
#     end
    
#     return df
# end

function prepare_cat_plot(dfa::DataFrame; stat::Symbol=:volume, 
                          rt::Symbol=:any, cvt::Symbol=:any,
                          date_cols::Array{Symbol, 1}=[:trd_exctn_yr, :trd_exctn_qtr])
    cond = .&(dfa[:, :sbm] .!= :both, dfa[:, :rt] .== :any, dfa[:, :cvt] .== :any)
    df = dfa[cond, :]
    rt_renamer(x) = x == :any ? "ANY" : x == :ig ? "Investment Grade" : "High Yield"
    sbm_renamer(x) = uppercase(string(x))

    df[!, :period] .= !isempty(date_cols) ? get_period(dfa, date_cols) : ""
    
    header_cols = [:period, :sbm, :rt, :cvt]
    statd = Dict(:count => ["count", :_trd_count], 
                 :volume => ["vol", :_trd_vol_tr],
                 :bonds => ["bond", :_bonds],
                 :issuers => ["issuers", :_issuers])

    stat_cols = [Symbol(x) for x in names(df) if .&(occursin("cg", x), occursin(statd[stat][1], x))] 
    col_ord = vcat(header_cols, stat_cols)

    # Get Volume by SBM
    any_cond = .&(df[:, :sbm] .== :any, [df[:, x] .== :any for x in [:rt, :cvt]]...)
    ats_cond = .&(df[:, :sbm] .== :ats, [df[:, x] .== :any for x in [:rt, :cvt]]...)
    otc_cond = .&(df[:, :sbm] .== :otc, [df[:, x] .== :any for x in [:rt, :cvt]]...)

    vold = Dict{Symbol, Float64}(Symbol(:total, statd[stat][2]) => df[any_cond, Symbol(:total, statd[stat][2])][1],
                                 Symbol(:ats, statd[stat][2]) => df[ats_cond, Symbol(:total, statd[stat][2])][1],
                                 Symbol(:otc, statd[stat][2]) => df[otc_cond, Symbol(:total, statd[stat][2])][1])

    # Pivot Table
    df = stack(df[:, col_ord], Not([:period, :sbm, :rt, :cvt]))
    df = df[df[:, :sbm] .!= :any, :]

    # Covenant Categories
    var_renamer(x) = parse(Int64, rsplit(rsplit(string(x), "_")[1], "cg")[2])
    df[!, :cov_cat] = var_renamer.(df[:, :variable])

    # When the stat is volume, only total volume:
    if stat == :volume
        fr(x) = any([occursin(y, x) for y in ["mean", "media", "q25", "q75"]])
        df = df[fr.(df[:, :variable]) .== false, :]
    end
    
    # Covenant Category's (Vol, Bond, Issuer, Trd) Count as Percentage of Total Count
    df[:, :perc_total] = (df[:, :value]/vold[Symbol(:total, statd[stat][2])]) * 100

    # Covenant Category's Trade Volume as Percentage of Total Trade Volume
    # by Secondary Bond Market
    f(sbm, value) =  (value/ vold[Symbol(sbm, statd[stat][2]) ]) .* 100
    df[:,  :perc_sbm_total] = f.(df[:, :sbm], df[:, :value])
    
    return df
end

function prepare_num_cov_plot(df::DataFrame; stat::Symbol=:volume, 
                              vol_stat::Symbol=:total,
                              rt::Symbol=:any, cvt::Symbol=:any,
                              date_cols::Array{Symbol, 1}=[:trd_exctn_yr, :trd_exctn_qtr])

    # cond = .&(df[:, :sbm] .!= :any, df[:, :sbm] .!= :both)
    rt_renamer(x) = x == :any ? "ANY" : x == :ig ? "Investment Grade" : "High Yield"
    sbm_renamer(x) = uppercase(string(x))

    df[!, :period] .= !isempty(date_cols) ? PlotMod.get_period(df, date_cols) : ""
        
    header_cols = [:period, :sbm, :rt, :sum_num_cov]
    statd = Dict(:count => ["count", :trades_by_num_cov], 
                 :volume => ["vol", Symbol(vol_stat, :_vol_by_num_cov)],
                 :bonds => ["bond", :bonds_by_num_cov],
                 :issuers => ["issuers", :issuers_by_num_cov])

    stat_cols = statd[stat][2]
    col_ord = vcat(header_cols, stat_cols)

    # Get Volume by SBM and 
    condsd = Dict()    
    for sbm in [:any, :ats, :otc]
        condsd[sbm] = Dict()
        for rt in [:any, :ig, :hy]
            condsd[sbm][rt] = .&(df[:, :sbm] .== sbm, df[:, :rt] .== rt)
        end
    end

    vold = Dict{Symbol, Float64}()
    for sbm in [:any, :ats, :otc]
        for rt in [:any, :ig, :hy]
            vold[Symbol(:sbm_, sbm, :_rt_, rt,:_, statd[stat][2])] = sum(df[condsd[sbm][rt], statd[stat][2]])
        end
    end
        
    # Pivot Table [:period, :sbm, :rt]
    df = stack(df[:, col_ord], Not(header_cols))
    # df = df[df[:, :sbm] .!= :any, :]
        
    # Stat as Percentage of Total by Secondary Bond Market
    f1(sbm, value) =  (value/ vold[Symbol(:sbm_, sbm, :_rt_any_, statd[stat][2])]) .* 100
    df[!,  :perc_sbm_total] = f1.(df[:, :sbm], df[:, :value])    
        
    # Stat as Percentage of Total by Secondary Bond Market and Rating
    f2(sbm, rt, value) =  (value/ vold[Symbol(:sbm_, sbm, :_rt_, rt, :_, statd[stat][2])]) .* 100
    df[!,  :perc_sbm_rt_total] = f2.(df[:, :sbm], df[:, :rt], df[:, :value])
        
    return df
end
# }}}
# Vega Plot Functions{{{1
function sbm_vega_plt(df::DataFrame, col_var::String, 
                      x_var::String, y_var::String; 
                      col_title::String=" ",
                      col_var_type::String="nominal", 
                      col_sort::String="descending",
                      x_var_type::String="nominal", 
                      x_axis_title::String=" ",
                      y_axis_title::String=" ", 
                      legend_title::String=" ", 
                      color_scale::String="viridis",
                      spacing::Float64=10.,
                      # width::Int64=300,
                      width_step::Int64=0,
                      height=240, 
                      title::Array{String, 1}=[" "], 
                      title_alignment::String="center", 
                      title_anchor::String="middle",
                      title_font_size::Int64=14, 
                      title_offset::Int64=20, 
                      domainWidth::Int64=3,
                      opacity::Float64=0.7, 
                      save_plt::Bool=true,
                      plt_type::String="",
                      stats_var::Symbol=Symbol(""),
                      main_path::String="",
                      plt_dir::String="",
                      file_ext::String="eps")

    # plt_width = width_step > 0 ? @vlfrag(step=width_step) : width

    p = df |> @vlplot(:bar,
                      column={col_var, type=col_var_type, 
                              title=col_title, sort=col_sort},
                      y={y_var, axis={title=y_axis_title, grid=false, stack=nothing}},
                      x={x_var, type=x_var_type, axis={title=x_axis_title}},
                      color={x_var, legend={title=legend_title}, scale={scheme=color_scale}},
                      config={
                          view={stroke=:transparent},
                          axis={domainWidth=domainWidth},
                          opacity={value=opacity},
                      },
                      title={text=title, 
                             align=title_alignment, 
                             anchor=title_anchor, 
                             fontSize=title_font_size, 
                             offset=title_offset},
                      spacing=spacing,
                      width={step=width_step},
                      height=height
                    ) 

    if save_plt
        if any([isempty(string(x)) for x in [plt_type, stats_var, main_path, plt_dir]])
            println("WARNING: missing file information. Plot will not be saved!")
            println("Please pass plt_type, stats_var, main_path and plt_dir",
                    " to the plot function.")
        else
            period = string(df[1, :period])
            fpname = get_plt_fpath_name(plt_type, stats_var, 
                                        y_var, period,  
                                        main_path, plt_dir;
                                        file_ext=file_ext)
            p |> VegaLite.save(fpname)
        end
    end
    
    return p
end

function dual_vega_plt(df::DataFrame, col_var::String, 
                      x_var::String, y_var::String, row_var::String;
                      row_var_type::String="nominal",
                      row_var_title::String="", 
                      col_title::String=" ",
                      col_var_type::String="nominal", 
                      col_sort::String="descending",
                      x_var_type::String="nominal", 
                      x_axis_title::String=" ",
                      y_axis_title::String=" ", 
                      legend_title::String=" ", 
                      color_scale::String="viridis",
                      spacing::Float64=10.,
                      # width::Int64=300,
                      width_step::Int64=0,
                      height=240, 
                      title::Array{String, 1}=[" "], 
                      title_alignment::String="center", 
                      title_anchor::String="middle",
                      title_font_size::Int64=14, 
                      title_offset::Int64=20, 
                      domainWidth::Int64=3,
                      opacity::Float64=0.7, 
                      save_plt::Bool=true,
                      plt_type::String="",
                      stats_var::Symbol=Symbol(""),
                      main_path::String="",
                      plt_dir::String="",
                      file_ext::String="eps")

    # plt_width = width_step > 0 ? @vlfrag(step=width_step) : width

    p = df |> @vlplot(:bar,
                  column={col_var, type=col_var_type, 
                          title=col_title, sort=col_sort},
                  y={y_var, axis={title=y_axis_title, grid=false, stack=nothing}},
                  x={x_var, type=x_var_type, axis={title=x_axis_title}},
                  color={x_var, legend={title=legend_title}, scale={scheme=color_scale}},
                  config={
                      view={stroke=:transparent},
                      axis={domainWidth=domainWidth},
                      opacity={value=opacity},
                  },
                  row={row_var, type=row_var_type, header={title=row_var_title}},
                  title={text=title, 
                         align=title_alignment, 
                         anchor=title_anchor, 
                         fontSize=title_font_size, 
                         offset=title_offset},
                  spacing=spacing,
                  width={step=width_step},
                  height=height
                ) 
    
    if save_plt
        if any([isempty(string(x)) for x in [plt_type, stats_var, main_path, plt_dir]])
            println("WARNING: missing file information. Plot will not be saved!")
            println("Please pass plt_type, stats_var, main_path and plt_dir",
                    " to the plot function.")
        else
            period = string(df[1, :period])
            fpname = get_plt_fpath_name(plt_type, stats_var, 
                                        y_var, period,  
                                        main_path, plt_dir;
                                        dual_plt=true,
                                        file_ext=file_ext)
            p |> VegaLite.save(fpname)
        end
    end
    
    return p
end
# }}}
end
