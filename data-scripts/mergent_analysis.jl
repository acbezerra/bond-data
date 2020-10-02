# vim: set fdm=marker :

using DataFrames
using Printf
using Latexify
using StatsBase

# %% Corporate Bond Covenant Data Analysis {{{1

# Dollar-Denominated US Corporate Bonds Sample {{{2
# Group 1: Dollar-Denominated US Corporate Bonds
g1 = .&(mdf[!, :usd], mdf[:, :corp_bond])

# without Covenant Data
g11 = .&(g1, .!mdf[!, :cov_data])

# without Subsequent Data
g12 = .&(g1, .!mdf[!, :subseq_data])
# ===============================================

# Group 2: Group 1, excluding MTNs ==============
g2 = .&(g1, .!mdf[:, :mtn])

# Group 21: Group 2 without Covenant Data
g21 = .&(g2, .!mdf[!, :cov_data])

# Group 22: Group 2 w/ Missing Covenant Data
g22 = .&(g2, .!mdf[!, :subseq_data])
# ===============================================

# # Group 3: Group 2 without Covenant Data ========
# g3 = .&(g2, .!mdf[!, :cov_data])
#
# # without Covenant Data
# g31 = g3
#
# # Group 222: Group 22 w/ Missing Subsequent Data
# g32 = .&(g3, .!mdf[!, :subseq_data])
# # ===============================================

# Group 4: Group 2 excluding Bonds without Covenant & Subsequent Data ========
# g4 = mdf[!, :selected]
g4 = .&(mdf[!, :usd], mdf[!, :corp_bond], .!mdf[!, :mtn],
        .&(.!mdf[!, :cov_data], .!mdf[!, :subseq_data]) .== false)

# without Covenant Data
g41 = .&(g4, .!mdf[!, :cov_data])

# Group 222: Group 22 w/ Missing Subsequent Data
g42 = .&(g4, .!mdf[!, :subseq_data])
# ===============================================

# Unique cusip
uc(cond) = size(unique(mdf[cond, :COMPLETE_CUSIP]), 1)

cpcdad = Dict(:usd_us_corp => Dict(:ex_mtn => false, :ex_miss_cov => false,
                                   :ex_miss_cov_subseq => false,
                                   :no_cov_data => uc(g11),
                                   :no_subseq_data => uc(g12),
                                   :total => uc(g1)),
              :usd_us_corp_no_mtn => Dict(:ex_mtn => true, :ex_miss_cov => false,
                                          :ex_miss_cov_subseq => false,
                                          :no_cov_data => uc(g21),
                                          :no_subseq_data => uc(g22),
                                          :total => uc(g2)),
              # :usd_us_corp_no_mtn_no_cov => Dict(:ex_mtn => true, :ex_miss_cov => true,
              #                                    :ex_miss_cov_subseq => false,
              #                                    :no_cov_data => uc(g31),
              #                                    :no_subseq_data => uc(g32),
              #                                    :total => uc(g3)),
              :usd_us_corp_ex_no_cov_no_subseq => Dict(:ex_mtn => true, :ex_miss_cov => false,
                                                       :ex_miss_cov_subseq => true,
                                                       :no_cov_data => uc(g41),
                                                       :no_subseq_data => uc(g42),
                                                       :total => uc(g4)))

row_order = [:usd_us_corp, :usd_us_corp_no_mtn, :usd_us_corp_ex_no_cov_no_subseq]
tmp = vcat([DataFrame(cpcdad[k]) for k in row_order]...)
tmp[:, :no_cov_perc] .= (tmp[:, :no_cov_data] ./ tmp[:, :total]) .* 1e2
tmp[:, :no_subseq_perc] .= (tmp[:, :no_subseq_data] ./ tmp[:, :total]) .* 1e2
col_order = [:ex_mtn, :ex_miss_cov, :ex_miss_cov_subseq, :total, :no_cov_data, :no_cov_perc,
             :no_subseq_data, :no_subseq_perc]
tmp[:, col_order]

println("Dollar-Denominated US Corporate Bond Sample")
print(latexify(tmp[:, col_order]))

num_selected_bonds = size(unique(mdf[mdf[:, :selected], :COMPLETE_CUSIP]), 1)
num_bonds = size(unique(mdf[.&(mdf[:, :usd], mdf[:, :corp_bond]), :COMPLETE_CUSIP]), 1)
perc_selected_bonds = (num_selected_bonds/num_bonds) * 100
println("# of selected bonds (including selected MTNs): ", num_selected_bonds, " out of ",
        num_bonds, " (", perc_selected_bonds, "%)")

num_selected_mtns = size(unique(mdf[.&(mdf[:, :selected], mdf[:, :mtn]), :COMPLETE_CUSIP]), 1)
num_mtns = size(unique(mdf[.&(mdf[:, :usd], mdf[:, :corp_bond], mdf[:, :mtn]), :COMPLETE_CUSIP]), 1)
perc_selected_mtns = (num_selected_mtns/num_mtns) * 100
println("# of selected MTNs: ", num_selected_mtns, " out of ", num_mtns, " (", perc_selected_mtns, "%)")

# }}}
# Convertible Bonds{{{2
conv_select = .&(mdf[:, :selected], mdf[:, :convertible])
num_select_bonds = size(unique(mdf[mdf[:, :selected], :COMPLETE_CUSIP]), 1)
num_conv_bonds = size(unique(mdf[conv_select, :COMPLETE_CUSIP]), 1)
perc_conv_bonds = (num_conv_bonds/num_select_bonds) * 100
println("# of convertible bonds among selected bonds: ", num_conv_bonds,
        " (", perc_conv_bonds, "%)")

# }}}
# MTNs filed under Rule 415 {{{2
cond = .&(mdf[:, :usd], mdf[:, :corp_bond], mdf[:, :mtn])
tmpdf = DataFrame(StatsBase.countmap(unique(mdf[cond, [:COMPLETE_CUSIP, :RULE_415_REG]])[:, :RULE_415_REG]))
tmpdf[:, :total] .= sum.(skipmissing.(eachrow(tmpdf)))
tmpdf[:, :perc_y] .= (tmpdf[:, :Y]./tmpdf[:, :total]).* 100
println("Dollar-Denominated Corporate MTNs filed under Rule 415:")
print(latexify(tmpdf))
# }}}
# }}}
