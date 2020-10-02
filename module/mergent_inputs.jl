# vim: set fdm=marker :

mergent_dir="MERGENT"
mergent_file_prefix="mergent_fisd"

# MERGENT COLUMNS {{{1
# NOT IN THE DATASET {{{2
# AGENT {{{3
agent_cols = Array{Symbol, 1}([:AGENT_ID,
                               :LEGAL_NAME,
                               :ADDR1,
                               :ADDR2,
                               :CITY,
                               :STATE,
                               :ZIPCODE,
                               :PROVINCE,
                               :COUNTRY,
                               :MAIN_PHONE,
                               :MAIN_FAX,
                               :NOTE])
# }}}
# COUPON_FORMULA_INDEX {{{3
coupon_for_ind_cols = Array{Symbol, 1}([:LINE_ID,
                                        :ISSUE_ID,
                                        :EFFECTIVE_DATE,
                                        :VALID_UNTIL_DATE,
                                        :FORMULA,
                                        :MINIMUM,
                                        :MAXIMUM])
# }}}
# }}}
# AMOUNT_OUTSTANDING {{{2
amount_out_cols = Array{Symbol, 1}([:ISSUE_ID,
                                    :ACTION_TYPE,
                                    :EFFECTIVE_DATE,
                                    :ACTION_PRICE,
                                    :ACTION_AMOUNT,
                                    :AMOUNT_OUTSTANDING])
# }}}
# BONDHOLDER_PROTECTIVE {{{2
bondh_prot_cols = Array{Symbol, 1}([:ISSUE_ID,
                                    :NEGATIVE_PLEDGE_COVENANT,
                                    :COVENANT_DEFEAS_WO_TAX_CONSEQ,
                                    :LEGAL_DEFEASANCE,
                                    :DEFEASANCE_WO_TAX_CONSEQ,
                                    :CROSS_DEFAULT,
                                    :CROSS_ACCELERATION,
                                    :CHANGE_CONTROL_PUT_PROVISIONS,
                                    :VOTING_POWER_PERCENTAGE,
                                    :VOTING_POWER_PERCENTAGE_ERP,
                                    :RATING_DECLINE_TRIGGER_PUT,
                                    :RATING_DECLINE_PROVISION,
                                    :DECLINING_NET_WORTH,
                                    :DECLINING_NET_WORTH_TRIGGER,
                                    :DECLINING_NET_WORTH_PERCENTAGE,
                                    :DECLINING_NET_WORTH_PROVISIONS,
                                    :AFTER_ACQUIRED_PROPERTY_CLAUSE,
                                    :ECONOMIC_COV_DEF,
                                    :ASSET_SALE_CLAUSE])
# }}}
# CHANGE_FORMULA {{{2
change_formula_cols =  Array{Symbol, 1}([:ISSUE_ID,
                                         :FIX_FREQUENCY,
                                         :DETERMINATION_DATE,
                                         :GREATER_OF,
                                         :LESSER_OF,
                                         :SEE_NOTE,
                                         :RESET_DATE,
                                         :DETERMINATION_DATE_ORIG,
                                         :RESET_DATE_ORIG])
# }}}
# CONVERTIBLE {{{2
convertible_cols = Array{Symbol, 1}([:ISSUE_ID,
                                    :CONV_COMMOD_ISSUER,
                                    :CONV_COMMOD_TYPE,
                                    :CONV_COMMOD_CUSIP,
                                    :EXCHANGE,
                                    :TICKER,
                                    :CONV_PRICE,
                                    :QTY_OF_COMMOD,
                                    :PERCENT_OF_OUTSTANDING_COMMOD,
                                    :CONV_CASH,
                                    :CONV_EFF_DATE,
                                    :CONV_EXP_DATE,
                                    :DILUTION_PROTECTION,
                                    :COMMOD_PRICE,
                                    :CONV_PREMIUM,
                                    :CONV_REDEMP_EXCEPTION,
                                    :CONV_REDEMP_DATE,
                                    :CONV_PRICE_PERCENT,
                                    :CONV_PART_TRADE_DAYS,
                                    :CONV_TOTAL_TRADE_DAYS,
                                    :CONV_PERIOD_SPEC,
                                    :CONV_PERIOD_DAYS,
                                    :AGENT_ID,
                                    :SHARES_OUTSTANDING,
                                    :ORIG_CONV_PRICE,
                                    :ORIG_COMMOD_PRICE,
                                    :ORIG_CONV_PREMIUM,
                                    :ORIG_SHARES_OUTSTANDING,
                                    :ORIG_PERCENT_OUTSTANDING_COM,
                                    :ORIG_QTY_OF_COMMOD,
                                    :AS_OF_DATE,
                                    :REASON,
                                    :CHANGE_DATE,
                                    :SPLIT_DATE,
                                    :SPLIT_RATIO,
                                    :CONDITIONAL_CONV_TERMS,
                                    :SOFT_CALL_MAKE_WHOLE,
                                    :PEPS,
                                    :PERCS,
                                    :CONV_PROHIBITED_FROM,
                                    :CONVERT_ON_CALL])

# CONVERTIBLE ADDITIONAL TERMS {{{3
convertible_add_cols = Array{Symbol, 1}([:ISSUE_ID,
                                        :COCO_START_DATE,
                                        :COCO_END_DATE,
                                        :COCO_INITIAL_TRIGGER_PERCENT,
                                        :COCO_TRIGGER_EXPRESSED_AS,
                                        :COCO_CHANGE_RATE,
                                        :COCO_MIN_TRIGGER_LEVEL,
                                        :COCO_CHANGE_FREQUENCY,
                                        :COCO_TRADE_DAYS,
                                        :COCO_TRADE_DAYS_IN_PREVIOUS,
                                        :SC_MAKE_WHOLE_START_DATE,
                                        :SC_MAKE_WHOLE_END_DATE,
                                        :SC_MAKE_WHOLE_DECREMENT_TYPE,
                                        :SC_MAKE_WHOLE_INITIAL_AMOUNT,
                                        :SC_MAKE_WHOLE_CHANGE_PERCENT,
                                        :PEPS_MAX_CONVERSION_RATIO,
                                        :PEPS_MIN_CONVERSION_RATIO,
                                        :PEPS_HIGHER_PRICE,
                                        :PEPS_LOWER_PRICE,
                                        :PEPS_ISSUE_PRICE,
                                        :PERCS_MAX_PAYOFF])
# }}}
# CONVERTIBLE_HISTORY (ALREADY PRESENT IN CONVERTIBLE) {{{3
convertible_hist_cols = Array{Symbol, 1}([:ISSUE_ID,
                                        :AS_OF_DATE,
                                        :CONV_COMMOD_ISSUER,
                                        :AGENT_ID,
                                        :CONV_PRICE,
                                        :QTY_OF_COMMOD,
                                        :COMMOD_PRICE,
                                        :CONV_PREMIUM,
                                        :SHARES_OUTSTANDING,
                                        :PERCENT_OF_OUTSTANDING_COMMOD,
                                        :SPLIT_DATE,
                                        :SPLIT_RATIO])
# }}}
# CONVERTIBLE_ISSUER_HISTORY (ALiREADY PRESENT IN CONVERTIBLE) {{{3
convertible_issuer_hist_cols = Array{Symbol, 1}([:ISSUE_ID,
                                                :AS_OF_DATE,
                                                :CONV_COMMOD_ISSUER,
                                                :AGENT_ID,
                                                :CONV_COMMOD_TYPE,
                                                :EXCHANGE,
                                                :CONV_COMMOD_CUSIP,
                                                :TICKER,
                                                :REASON,
                                                :DILUTION_PROTECTION,
                                                :CONV_EFF_DATE,
                                                :CONV_EXP_DATE,
                                                :CONV_CASH])
# }}}
# }}}
# COUPON_INFO {{{2
coupon_info_cols = Array{Symbol, 1}([:ISSUE_ID,
                                    :DATED_DATE,
                                    :FIRST_INTEREST_DATE,
                                    :INTEREST_FREQUENCY,
                                    :COUPON,
                                    :PAY_IN_KIND,
                                    :PAY_IN_KIND_EXP_DATE,
                                    :COUPON_CHANGE_INDICATOR,
                                    :DAY_COUNT_BASIS,
                                    :LAST_INTEREST_DATE,
                                    :NEXT_INTEREST_DATE]) # reserved for future use
# }}}
# EXCHANGE_LISTING (ALREADY PRESENT IN CONVERTIBLE) {{{2
exchange_list_cols = Array{Symbol, 1}([:EXCHANGE,
                                       :ISSUER_ID,
                                       :TICKER])
# }}}
# FOREIGN_CURRENCY {{{2
foreign_curr_cols = Array{Symbol, 1}([:CURRENCY,
                                      :ISSUE_ID,
                                      :AMT_OFFERED,
                                      :CONVERSION_RATE])
# }}}
# ISSUE {{{2
# ISSUE - main {{{3
issue_main_cols = Array{Symbol, 1}([:ISSUE_ID,
                                    :ISSUER_ID,
                                    :ISSUER_CUSIP,
                                    :ISSUE_CUSIP,
                                    :MATURITY,
                                    :SECURITY_LEVEL,
                                    :SECURITY_PLEDGE,
                                    :ENHANCEMENT,
                                    :CONVERTIBLE,
                                    :ASSET_BACKED,
                                    :ISSUE_OFFERED_GLOBAL,
                                    :GROSS_SPREAD,
                                    :RULE_144A,
                                    :OFFERING_AMT,
                                    :OFFERING_DATE,
                                    :PRINCIPAL_AMT,
                                    :COVENANTS,
                                    :DEFEASANCE_TYPE,
                                    :REDEEMABLE,
                                    :REFUND_PROTECTION,
                                    :PUTABLE,
                                    :ACTIVE_ISSUE,
                                    :PRIVATE_PLACEMENT,
                                    :BOND_TYPE,
                                    :PERPETUAL,
                                    :EXCHANGEABLE,
                                    :FUNGIBLE,
                                    :REGISTRATION_RIGHTS,
                                    :PREFERRED_SECURITY,
                                    :PRINCIPAL_PROTECTED,              # not in the dataset
                                    :PRINCIPAL_PROTECTED_PERCENTAGE])  # not in the dataset
# }}}
# ISSUE - OTHER {{{3
issue_other_cols = Array{Symbol, 1}([:PROSPECTUS_ISSUER_NAME,
                                     :ISSUE_NAME,
                                     :COUPON_TYPE,
                                     :MTN,
                                     :YANKEE,
                                     :CANADIAN,
                                     :OID,
                                     :FOREIGN_CURRENCY,
                                     :SLOB,
                                     :SETTLEMENT_TYPE,
                                     :SELLING_CONCESSION,
                                     :REALLOWANCE,
                                     :COMP_NEG_EXCH_DEAL,
                                     :RULE_415_REG,
                                     :SEC_REG_TYPE1,
                                     :SEC_REG_TYPE2,
                                     :TREASURY_SPREAD,
                                     :TREASURY_MATURITY,
                                     :OFFERING_PRICE,
                                     :OFFERING_YIELD,
                                     :DELIVERY_DATE,
                                     :UNIT_DEAL,
                                     :FORM_OF_OWN,
                                     :DENOMINATION,
                                     :DEFEASED,
                                     :DEFEASED_DATE,
                                     :DEFAULTED,
                                     :TENDER_EXCH_OFFER,
                                     :REFUNDING_DATE,
                                     :OVERALLOTMENT_OPT,
                                     :ANNOUNCED_CALL,
                                     :DEP_ELIGIBILITY,
                                     :SUBSEQUENT_DATA,
                                     :PRESS_RELEASE,
                                     :ISIN,
                                     :SEDOL])
# }}}
# ISSUE_AFFECTED {{{3
issue_affected_cols = Array{Symbol, 1}([:ISSUER_ID_AFFECTED,
                                        :FILING_DATE,
                                        :ISSUE_ID,
                                        :SETTLEMENT])
# }}}
# }}}
# ISSUER {{{2
issuer_cols = Array{Symbol, 1}([:ISSUER_ID,
                                :AGENT_ID,
                                :CUSIP_NAME,
                                :INDUSTRY_GROUP,
                                :INDUSTRY_CODE,
                                :ESOP,
                                :IN_BANKRUPTCY,
                                :PARENT_ID,
                                :NAICS_CODE,
                                :COUNTRY_DOMICILE])

# ISSUER_RESTRICTIVE {{{3
issuer_restrictive_cols = Array{Symbol, 1}([:ISSUE_ID,
                                            :CONSOLIDATION_MERGER,
                                            :INVESTMENTS,
                                            :MAINTENANCE_NET_WORTH,
                                            :RESTRICTED_PAYMENTS,
                                            :SALE_ASSETS,
                                            :SENIOR_DEBT_ISSUANCE,
                                            :STOCK_ISSUANCE_ISSUER,
                                            :STOCK_TRANSFER_SALE_DISP,
                                            :SUBORDINATED_DEBT_ISSUANCE,
                                            :TRANSACTION_AFFILIATES,
                                            :NET_EARNINGS_TEST_ISSUANCE,
                                            :DIVIDENDS_RELATED_PAYMENTS_IS,
                                            :FUNDED_DEBT_IS,
                                            :INDEBTEDNESS_IS,
                                            :LIENS_IS,
                                            :SALES_LEASEBACK_IS,
                                            :FIXED_CHARGE_COVERAGE_IS,
                                            :LEVERAGE_TEST_IS])
# }}}
# ISSUER_SIC_CODE {{{3
# ISSUER_SIC_CODE - FOUND 
issuer_sic_found_cols = Array{Symbol, 1}([:ISSUER_ID, :SIC_CODE])

# ISSUER_SIC_CODE - MISSING
issuer_sic_miss_cols = [:PRIMARY_SIC]
# }}}
# }}}
# OTHER_SECURITY {{{2
other_sec_cols = Array{Symbol, 1}([:OTHER_SEC_TYPE,
                                   :ISSUE_ID,
                                   :OTHER_SEC_ISSUER,
                                   :SEC_CUSIP,
                                   :QUANTITY,
                                   :DATE_TRANSFERABLE,
                                   :DATE_SUBJ_ADJUSTMENT,
                                   :MARKET_PRICE,
                                   :ALLOCATED_OFFERING_PRICE_OTHER])
# }}}
# OVERALLOTMENT {{{2
overallotment_cols = Array{Symbol, 1}([:ISSUE_ID,
                                       :OVERALLOTMENT_EXPIRATION_DATE,
                                       :EXERCISED,
                                       :EXERCISED_DATE,
                                       :AMOUNT])
# }}}
# PUT {{{2
put_cols = Array{Symbol, 1}([:ISSUE_ID,
                             :NOTIFICATION_PERIOD,
                             :NEXT_PUT_DATE,
                             :NEXT_PUT_PRICE])
# }}}
# RATING {{{2
rating_cols = Array{Symbol, 1}([:ISSUE_ID,
                                :RATING_TYPE,
                                :RATING_DATE,
                                :RATING,
                                :RATING_STATUS,
                                :REASON,
                                :RATING_STATUS_DATE,
                                :INVESTMENT_GRADE])
# }}}
# SUBS_RESTRICTIVE {{{2
subs_rest_cols = Array{Symbol, 1}([:ISSUE_ID,
                                   :DIVIDENDS_RELATED_PAYMENTS_SUB,
                                   :FUNDED_DEBT_SUB,
                                   :INDEBTEDNESS_SUB,
                                   :INVESTMENTS_UNRESTRICTED_SUBS,
                                   :SALES_LEASEBACK_SUB,
                                   :LIENS_SUB,
                                   :FIXED_CHARGE_COVERAGE_SUB,
                                   :LEVERAGE_TEST_SUB,
                                   :BORROWING_RESTRICTED,
                                   :STOCK_ISSUANCE,
                                   :PREFERRED_STOCK_ISSUANCE,
                                   :SALE_XFER_ASSETS_UNRESTRICTED,
                                   :SUBSIDIARY_REDESIGNATION,
                                   :SUBSIDIARY_GUARANTEE])
# }}}
# UNIT {{{2
unit_cols = Array{Symbol, 1}([:ISSUE_ID,
                              :UNIT_CUSIP,
                              :TOTAL_UNITS_OFFERED,
                              :PRINCIPAL_AMT_PER_UNIT,
                              :ALLOCATED_OFFERING_PRICE_UNIT])
# }}}
# }}}
# Lists of Variables {{{1
mergent_cols_dict = Dict{Symbol,Array{Symbol,1}}(:AMOUNT_OUTSTANDING => amount_out_cols,
                                                 :BONDHOLDER_PROTECTIVE => bondh_prot_cols,
                                                 :CHANGE_FORMULA => change_formula_cols,
                                                 :CONVERTIBLE => convertible_cols,
                                                 :CONVERTIBLE_ADDITIONAL_TERMS => convertible_add_cols,
                                                 :CONVERTIBLE_HISTORY => convertible_hist_cols,
                                                 :CONVERTIBLE_ISSUER_HISTORY => convertible_issuer_hist_cols,
                                                 :COUPON_INFO => coupon_info_cols,
                                                 :EXCHANGE_LISTING => exchange_list_cols,
                                                 :FOREIGN_CURRENCY => foreign_curr_cols,
                                                 :ISSUE_MAIN => issue_main_cols,
                                                 :ISSUE_OTHER => issue_other_cols,
                                                 :ISSUE_AFFECTED => issue_affected_cols,
                                                 :ISSUER => issuer_cols,
                                                 :ISSUER_RESTRICTIVE => issuer_restrictive_cols,
                                                 :ISSUER_SIC_FOUND => issuer_sic_found_cols,
                                                 :OTHER_SECURITY => other_sec_cols,
                                                 :OVERALLOTMENT => overallotment_cols,
                                                 :PUT => put_cols,
                                                 :RATING => rating_cols,
                                                 :SUBS_RESTRICTIVE => subs_rest_cols,
                                                 :UNIT => unit_cols)

# ID'ED and UN-ID'ED COLS {{{2
mergent_ided_cols = unique(vcat(amount_out_cols,
                                bondh_prot_cols,
                                change_formula_cols,
                                convertible_cols,
                                convertible_add_cols,
                                # convertible_hist_cols,
                                # convertible_issuer_hist_cols,
                                coupon_info_cols,
                                # exchange_list_cols,
                                foreign_curr_cols,
                                issue_main_cols,
                                issue_other_cols,
                                issue_affected_cols,
                                issuer_cols,
                                issuer_restrictive_cols,
                                issuer_sic_found_cols,
                                other_sec_cols,
                                overallotment_cols,
                                put_cols,
                                rating_cols,
                                subs_rest_cols,
                                unit_cols))

mergent_unided_cols = [:REASON_1,
                       :ISSUER_ID_1,
                       :PROSPECTUS_ISSUER_NAME_1,
                       :ISSUER_CUSIP_1,
                       :ISSUE_CUSIP_1,
                       :ISSUE_NAME_1,
                       :MATURITY_1,
                       :OFFERING_DATE_1,
                       :COMPLETE_CUSIP_1]
# }}}
# }}}
# Kept and Discarded Variables {{{1

# By category {{{2
amount_out_keep= Array{Symbol, 1}([:EFFECTIVE_DATE,
                                   :AMOUNT_OUTSTANDING])

# Convert to BOOLEANS!
bondh_prot_keep = Array{Symbol, 1}([:NEGATIVE_PLEDGE_COVENANT,
                                    :COVENANT_DEFEAS_WO_TAX_CONSEQ,
                                    :LEGAL_DEFEASANCE,
                                    :DEFEASANCE_WO_TAX_CONSEQ,
                                    :CROSS_DEFAULT,
                                    :CROSS_ACCELERATION,
                                    :CHANGE_CONTROL_PUT_PROVISIONS,
                                    :VOTING_POWER_PERCENTAGE,         # (NON-BOOLEAN)
                                    :VOTING_POWER_PERCENTAGE_ERP,     # (NON-BOOLEAN)
                                    :RATING_DECLINE_TRIGGER_PUT,
                                    # :RATING_DECLINE_PROVISION,
                                    :DECLINING_NET_WORTH,
                                    # :DECLINING_NET_WORTH_TRIGGER,
                                    # :DECLINING_NET_WORTH_PERCENTAGE,
                                    # :DECLINING_NET_WORTH_PROVISIONS,
                                    :AFTER_ACQUIRED_PROPERTY_CLAUSE,
                                    :ECONOMIC_COV_DEF,
                                    :ASSET_SALE_CLAUSE])

# -> although these conditions can add complication,
# they are not necessarily added to give investors
# protection against adverse selection?
change_formula_keep =  Array{Symbol, 1}([:FIX_FREQUENCY])

convertible_keep = Array{Symbol, 1}([:CONV_COMMOD_TYPE,
                                     :CONV_CASH,
                                     :DILUTION_PROTECTION,
                                     :CONV_REDEMP_EXCEPTION,
                                     :CONDITIONAL_CONV_TERMS,
                                     :SOFT_CALL_MAKE_WHOLE,
                                     :PEPS,
                                     :PERCS,
                                     :CONVERT_ON_CALL])

# JUST create an indicator for convertible conversion:
convertible_add_keep = Array{Symbol, 1}([:COCO_TRIGGER_EXPRESSED_AS])

coupon_info_keep = Array{Symbol, 1}([:PAY_IN_KIND,
                                     :COUPON_CHANGE_INDICATOR])

issue_main_keep = Array{Symbol, 1}([:ISSUE_ID,
                                    :ISSUER_ID,
                                    :ISSUER_CUSIP,
                                    :ISSUE_CUSIP,
                                    :MATURITY,
                                    :SECURITY_LEVEL,
                                    :SECURITY_PLEDGE,
                                    :ENHANCEMENT,
                                    :CONVERTIBLE,
                                    :ASSET_BACKED,
                                    # :ISSUE_OFFERED_GLOBAL,
                                    # :GROSS_SPREAD,
                                    :RULE_415_REG,
                                    :RULE_144A,
                                    :OFFERING_AMT,
                                    :OFFERING_DATE,
                                    :PRINCIPAL_AMT,
                                    :COVENANTS,
                                    :DEFEASANCE_TYPE,
                                    :REDEEMABLE,
                                    :REFUND_PROTECTION,
                                    :PUTABLE,
                                    # :ACTIVE_ISSUE,
                                    :PRIVATE_PLACEMENT,
                                    :BOND_TYPE,
                                    :PERPETUAL,
                                    :EXCHANGEABLE,
                                    :FUNGIBLE,
                                    :REGISTRATION_RIGHTS,
                                    :PREFERRED_SECURITY])
                                    # :PRINCIPAL_PROTECTED,              # not in the dataset
                                    # :PRINCIPAL_PROTECTED_PERCENTAGE])  # not in the dataset

issue_other_keep = Array{Symbol, 1}([:COUPON_TYPE,
                                     :FOREIGN_CURRENCY,
                                     :DENOMINATION,
                                     :DEFEASED,
                                     :DEFEASED_DATE,
                                     :DEFAULTED,
                                     :ISIN])

issuer_keep = Array{Symbol, 1}([:IN_BANKRUPTCY])

# IMPORTANT
issuer_restrictive_keep = Array{Symbol, 1}([:CONSOLIDATION_MERGER,
                                            :DIVIDENDS_RELATED_PAYMENTS_IS,
                                            :FUNDED_DEBT_IS,
                                            :INDEBTEDNESS_IS,
                                            :INVESTMENTS,
                                            :LIENS_IS,
                                            :MAINTENANCE_NET_WORTH,
                                            :RESTRICTED_PAYMENTS,
                                            :SALES_LEASEBACK_IS,
                                            :SALE_ASSETS,
                                            :SENIOR_DEBT_ISSUANCE,
                                            :STOCK_ISSUANCE_ISSUER,
                                            :STOCK_TRANSFER_SALE_DISP,
                                            :SUBORDINATED_DEBT_ISSUANCE,
                                            :TRANSACTION_AFFILIATES,
                                            :NET_EARNINGS_TEST_ISSUANCE,
                                            :FIXED_CHARGE_COVERAGE_IS,
                                            :LEVERAGE_TEST_IS])

put_keep = Array{Symbol, 1}([:NEXT_PUT_DATE])

# Compare my ig variable with Mergent's Investment Grade variable
rating_keep = Array{Symbol, 1}([:RATING_TYPE,
                                :RATING_DATE,
                                :RATING,
                                :INVESTMENT_GRADE])

# COVENANTS => RESTRICTIONS on SUBSIDIARIES
subs_rest_keep = Array{Symbol, 1}([:DIVIDENDS_RELATED_PAYMENTS_SUB,
                                   :BORROWING_RESTRICTED,
                                   :FUNDED_DEBT_SUB,
                                   :INDEBTEDNESS_SUB,
                                   :STOCK_ISSUANCE,
                                   :PREFERRED_STOCK_ISSUANCE,
                                   :INVESTMENTS_UNRESTRICTED_SUBS,
                                   :SALE_XFER_ASSETS_UNRESTRICTED,
                                   :SUBSIDIARY_REDESIGNATION,
                                   :SUBSIDIARY_GUARANTEE,
                                   :SALES_LEASEBACK_SUB,
                                   :LIENS_SUB,
                                   :FIXED_CHARGE_COVERAGE_SUB,
                                   :LEVERAGE_TEST_SUB])

user_defined_keep = vcat([:rating_date, :usd, :mtn,
                          :corp_bond, :cov_data, :subseq_data,
                          :selected, :convertible, :covenant], 
                         [Symbol(:cg, x) for x in 1:15])
# }}}
# Group all variables to be kept {{{2
mergent_vars_keep = unique(vcat([:COMPLETE_CUSIP], 
                                 amount_out_keep,
                                 bondh_prot_cols,       # ALL COLS
                                 change_formula_keep,
                                 convertible_cols,      # ALL COLS
                                 convertible_add_cols,  # ALL COLS
                                 coupon_info_keep,
                                 issue_main_keep,
                                 issue_other_keep,
                                 issuer_cols,
                                 issuer_restrictive_keep,
                                 put_keep,
                                 rating_keep,
                                 subs_rest_keep,
                                 user_defined_keep))
# }}}
# Further Analysis {{{2 
non_bool_cols = vcat([:COMPLETE_CUSIP],
                     amount_out_keep, bondh_prot_cols,
                     # [:VOTING_POWER_PERCENTAGE, :VOTING_POWER_PERCENTAGE_ERP], # bondh_prot_keep
                     change_formula_keep,
                     convertible_cols,
                     # [:CONV_COMMOD_TYPE, :CONV_CASH],                          # convertible_keep
                     convertible_add_cols,
                     [:COUPON_CHANGE_INDICATOR],                               # coupon_info_keep
                     [:ISSUE_ID, :ISSUER_ID, :ISSUER_CUSIP, 
                      :ISSUE_CUSIP,  :MATURITY,
                      :SECURITY_LEVEL, :OFFERING_AMT, :OFFERING_DATE, 
                      :PRINCIPAL_AMT, :DEFEASANCE_TYPE, :BOND_TYPE],           # issue_main_keep
                     [:COUPON_TYPE, :FOREIGN_CURRENCY, 
                      :DENOMINATION, :DEFEASED_DATE, :ISIN],                   # issue_other_keep
                     put_keep,
                     [:RATING_TYPE, :RATING_DATE, :RATING],                    # rating_keep
                     user_defined_keep)
# MISSING OR N
investigate_cols = [:SECURITY_PLEDGE]              # issue_main_keep

missing_or_n_cols = [:LEVERAGE_TEST_SUB]           # subs_rest_keep

# ONLY Missing
only_missing_cols = [ ] 

# ONLY N
only_n_cols = [ ]

# Select Columns
not_convert_cols = vcat(non_bool_cols, investigate_cols, 
                        missing_or_n_cols, only_missing_cols, only_n_cols)
convert2bool_cols = [x for x in DataMod.mergent_vars_keep if !(x in not_convert_cols)]

# Variables not included
# :ISSUE_OFFERED_GLOBAL
# :UNIT_DEAL            (not)
# :FORM_OF_OWN          (not)
# :PRINCIPAL_PROTECTED  (not)
# :ISSUE_DEFAULT        (not)
# CALLABLE => is NOT covenant

# UNAVAILABLE->
# 1. ISSUE_ENHANCEMENT
# 2. POISON_PUT_SCHEDULE
# 3. REDEMPTION
# 4. RELATED_ISSUES => FIND MISSING CUSIPS?
# 5. WARRANT ? (Not sure I need this
# }}}
# }}}
# BOND TYPE - US Corporate {{{1
corp_bond_types = Dict{Symbol, String}(:CCOV => "US Corporate Convertible",
                                       :CCPI => "US Corporate Inflation Indexed",
                                       :CDEB => "US Corporate Debentures",
                                       :CLOC => "US Corporate LOC Backed",
                                       :CMTN => "US Corporate MTN",
                                       :CMTZ => "US Corporate MTN Zero",
                                       :CP   => "US Corporate Paper",
                                       :CPAS => "US Corporate Pass Thru Trust",
                                       :CPIK => "US Corporate PIK Bond",
                                       :CS   => "US Corporate Strip",
                                       :CUIT => "US Corporate UIT",
                                       :CZ   => "US Corporate Zero",
                                       :UCID => "US Corporate Insured Debenture",
                                       :USBN => "US Corporate Bank Note")
# }}}
# Covenant Groups {{{1
covgr = Dict{Int64, String}(1 => "Dividend pmnt. restrs.",
                            2 => "Share repurchase restrs.",
                            3 => "Funded debt restrs.",
                            4 => "Subordinate debt restrs.",
                            5 => "Senior debt restrs.",
                            6 => "Secured debt restrs.",
                            7 => "Total leverage test",
                            8 => "Sale & leaseback",
                            9 => "Stock issue restrs.",
                            10 => "Rating & net wrth. trgs.",
                            11 => "Cross-default provisions",
                            12 => "Poison put",
                            13 => "Asset sale clause",
                            14 => "Invest. policy restrs",
                            15 => "Merger restrictions") 

vars2covgr = Dict{Symbol, Int64}(:DIVIDENDS_RELATED_PAYMENTS_IS   => 1, 
                                 :DIVIDENDS_RELATED_PAYMENTS_SUB  => 1, 
                                 :RESTRICTED_PAYMENTS             => 2, 
                                 :ECONOMIC_COV_DEF                => 2, 
                                 :COVENANT_DEFEAS_WO_TAX_CONSEQ   => 2, 
                                 :LEGAL_DEFEASANCE                => 2, 
                                 :DEFEASANCE_WO_TAX_CONSEQ        => 2, 
                                 :FUNDED_DEBT_IS                  => 3, 
                                 :FUNDED_DEBT_SUB                 => 3, 
                                 :SUBORDINATED_DEBT_ISSUANCE      => 4, 
                                 :SENIOR_DEBT_ISSUANCE            => 5, 
                                 :NEGATIVE_PLEDGE_COVENANT        => 6, 
                                 :SUBSIDIARY_GUARANTEE            => 6, 
                                 :MAINTENANCE_NET_WORTH           => 7, 
                                 :INDEBTEDNESS_IS                 => 7, 
                                 :FIXED_CHARGE_COVERAGE_IS        => 7, 
                                 :LEVERAGE_TEST_IS                => 7, 
                                 :NET_EARNINGS_TEST_ISSUANCE      => 7, 
                                 :BORROWING_RESTRICTED            => 7, 
                                 :INDEBTEDNESS_SUB                => 7, 
                                 :FIXED_CHARGE_COVERAGE_SUB       => 7, 
                                 :LEVERAGE_TEST_SUB               => 7, 
                                 :SALES_LEASEBACK_IS              => 8, 
                                 :SALES_LEASEBACK_SUB             => 8, 
                                 :STOCK_ISSUANCE_ISSUER           => 9, 
                                 :STOCK_ISSUANCE                  => 9, 
                                 :PREFERRED_STOCK_ISSUANCE        => 9, 
                                 :RATING_DECLINE_TRIGGER_PUT     => 10, 
                                 :RATING_DECLINE_PROVISION       => 10, 
                                 :DECLINING_NET_WORTH            => 10, 
                                 :DECLINING_NET_WORTH_TRIGGER    => 10, 
                                 :DECLINING_NET_WORTH_PERCENTAGE => 10, 
                                 :DECLINING_NET_WORTH_PROVISIONS => 10, 
                                 :CROSS_DEFAULT                  => 11, 
                                 :CROSS_ACCELERATION             => 11, 
                                 :CHANGE_CONTROL_PUT_PROVISIONS  => 12, 
                                 :VOTING_POWER_PERCENTAGE        => 12, 
                                 :VOTING_POWER_PERCENTAGE_ERP    => 12, 
                                 :ASSET_SALE_CLAUSE              => 13, 
                                 :SALE_ASSETS                    => 13, 
                                 :LIENS_IS                       => 13, 
                                 :AFTER_ACQUIRED_PROPERTY_CLAUSE => 13, 
                                 :SALE_XFER_ASSETS_UNRESTRICTED  => 13, 
                                 :LIENS_SUB                      => 13, 
                                 :INVESTMENTS                    => 14, 
                                 :TRANSACTION_AFFILIATES         => 14, 
                                 :STOCK_TRANSFER_SALE_DISP       => 14, 
                                 :INVESTMENTS_UNRESTRICTED_SUBS  => 14, 
                                 :SUBSIDIARY_REDESIGNATION       => 14, 
                                 :CONSOLIDATION_MERGER           => 15)
# }}}

