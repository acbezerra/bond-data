using DataFrames
using CSV
using Distributed

main_path = "/home/artur/BondPricing"
module_path = string(main_path, "/", "bond-data/")
include(string(joinpath(module_path, "data_module"), ".jl"))

# Capture Job Number
job_num = parse(Int, ARGS[1])

# Define Year and Quarter
min_yr = 2013
max_yr = 2019
yrqtr = DataFrames.crossjoin(DataFrame(:year => min_yr:max_yr), DataFrame(:qtr => 1:4))
yr = yrqtr[job_num, :year]
qtr = yrqtr[job_num, :qtr]

# Load Trace Object
dto = DataMod.data_obj_constructor()

# Load Clean DataFrame
proc_fpath = string(dto.tr.trace_path, "/", dto.tr.proc_dir)
proc_fname =string(dto.tr.proc_file_prefix, "_", yr, "_Q", qtr, ".csv")
df = @time DataFrame!(CSV.File(string(proc_fpath, "/", proc_fname), types=dto.tr.colsd))

# %% Step 1 - Filter Agency Trades
del_nca_trds = false  # non-commissioned agency trades
df = @time DataMod.filter_agency_trades(df;
                                        del_nca_trds=del_nca_trds)

# %% Step 2 - Filter Interdealer Trades
rename_rpt_party = true
df = @time DataMod.remove_interdealer_buyer_side_reports(df;
                                                         rename_rpt_party=rename_rpt_party)

# %% Step 3 - Remove trades executed under special conditions
del_wi_trds = true                # When-Issued basis
del_non_secondary_trds = true     # non-secondary market trades
del_special_trds = true           # special conditions
del_wap_trds = true               # weighted-average price
del_agu_locked_in_trds = false    # AGU/Locked-in trades
del_non_corp_trds = true          # Trades of non-corporate securities
df = @time DataMod.filter_special_conditions(df;
                                             del_wi_trds=del_wi_trds,
                                             del_non_secondary_trds=del_non_secondary_trds,
                                             del_special_trds=del_special_trds,
                                             del_wap_trds=del_wap_trds,
                                             del_agu_locked_in_trds=del_agu_locked_in_trds,
                                             del_non_corp_trds=del_non_corp_trds)

# %% Step 4 - Remove trades with odd settlement dates
df = @time DataMod.filter_days_to_settlement(df)


# Save DataFrame
fdf_fpath = string(dto.tr.trace_path, "/", dto.tr.filter_dir)
fdf_fname =string(dto.tr.filter_file_prefix, "_", yr, "_Q", qtr, ".csv")

println(string("Saving ", dto.tr.filter_file_prefix, "_", yr, "_Q", qtr, " dataframe..."))
CSV.write(string(fdf_fpath, "/", fdf_fname), df)

