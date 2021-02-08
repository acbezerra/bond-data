# vim: set fdm=marker :

# Folder Paths and Names {{{1
trace_dir="TRACE"
raw_files_dir="raw"
pre_proc_dir="pre-processed"
proc_dir="processed"
filter_dir="filtered"
trace_file_prefix="trace_enhanced"
cancel_trd_file_prefix="cancel_trd"
reverse_trd_file_prefix="reverse_trd"
original_trd_file_prefix="original_trd"
proc_file_prefix="processed"
# }}}
# TRACE Columns {{{1
cancel_df_cols=["cusip_id", "entrd_vol_qt", "rptd_pr",
                "trd_exctn_dt", "trd_exctn_tm", "trc_st",
                "rpt_side_cd", "cntra_mp_id", "msg_seq_nb"]

rev_df_cols=vcat([x for x in cancel_df_cols if x != "msg_seq_nb"],
                 "orig_msg_seq_nb")

trace_cols_dict = Dict{String, DataType}("cusip_id" => String,
                                         "bond_sym_id" => String,
                                         "company_symbol" => String,
                                         "trd_exctn_dt" => String,
                                         "trd_exctn_tm" => String,
                                         "trd_rpt_dt" => String,
                                         "trd_rpt_tm" => String,
                                         "msg_seq_nb" => Int64,
                                         "trc_st" => String,
                                         "scrty_type_cd" => Int64,
                                         "wis_fl" => String,
                                         "cmsn_trd" => String,
                                         "entrd_vol_qt" => Float64,
                                         "rptd_pr" => Float64,
                                         "yld_sign_cd" => String,
                                         "yld_pt" => Float64,
                                         "asof_cd" => String,
                                         "days_to_sttl_ct" => String,
                                         "sale_cndtn_cd" => String,
                                         "sale_cndtn2_cd" => String,
                                         "rpt_side_cd" => String,
                                         "buy_cmsn_rt" => Float64,
                                         "buy_cpcty_cd" => String,
                                         "sell_cmsn_rt" => Float64,
                                         "sell_cpcty_cd" => String,
                                         "cntra_mp_id" => String,
                                         "agu_qsr_id" => String,
                                         "spcl_trd_fl" => String,
                                         "trdg_mkt_cd" => String,
                                         "dissem_fl" => String,
                                         "orig_msg_seq_nb" => Int64,
                                         "bloomberg_identifier" => String,
                                         "sub_prdct" => String,
                                         "stlmnt_dt" => Int64,
                                         "trd_mod_3" => String,
                                         "trd_mod_4" => String,
                                         "rptg_party_type" => String,
                                         "lckd_in_ind" => String,
                                         "ats_indicator" => String,
                                         "pr_trd_dt" => Int64,
                                         "first_trade_ctrl_date" => Int64,
                                         "first_trade_ctrl_num" => Int64)
# }}}
# Kept and Discarded Variables  {{{1
trace_discard_dict = Dict{Symbol, DataType}(:bond_sym_id => String,
                                            :company_symbol => String,
                                            :scrty_type_cd => Int64,
                                            :trd_rpt_dt => String,
                                            :trd_rpt_tm => String,
                                            :msg_seq_nb => Int64,
                                            :trc_st => String,
                                            :scrty_type_cd => Int64,
                                            :wis_fl => String,
                                            :cmsn_trd => String,
                                            :yld_sign_cd => String,
                                            :yld_pt => Float64,
                                            :asof_cd => String,
                                            :days_to_sttl_ct => String,
                                            :sale_cndtn_cd => String,
                                            :sale_cndtn2_cd => String,
                                            :buy_cpcty_cd => String,
                                            :sell_cpcty_cd => String,
                                            :agu_qsr_id => String,
                                            :spcl_trd_fl => String,
                                            :trdg_mkt_cd => String,
                                            :dissem_fl => String,
                                            :orig_msg_seq_nb => Int64,
                                            :bloomberg_identifier => String,
                                            :sub_prdct => String,
                                            :stlmnt_dt => Int64,
                                            :trd_mod_3 => String,
                                            :trd_mod_4 => String,
                                            :lckd_in_ind => String,
                                            :pr_trd_dt => Int64,
                                            :first_trade_ctrl_date => Int64,
                                            :first_trade_ctrl_num => Int64)


trace_keep_dict = Dict{Symbol, DataType}(:cusip_id => String,
                                         :bond_sym_id => String,
                                         :company_symbol => String,
                                         :trd_exctn_dt => String,
                                         :trd_exctn_tm => String,
                                         :trd_exctn_yr => Int64,  
                                         :trd_exctn_mo => Int64,
                                         :entrd_vol_qt => Float64,
                                         :rptd_pr => Float64,
                                         :rpt_side_cd => String,
                                         :buy_cmsn_rt => Float64,
                                         :sell_cmsn_rt => Float64,
                                         :cntra_mp_id => String,
                                         :rptg_party_type => String,
                                         :ats_indicator => String)

# User-Defined
trace_ud_discard_dict = Dict{Symbol, DataType}(:stlmnt_date => Date)

trace_ud_keep_dict = Dict{Symbol, DataType}(:rpt_side_cpcty => String,
                                            :trd_exctn_date => Date,
                                            :rating_date => Date,
                                            :ig => Bool)
# }}}
