
##################################################
#Copy this file to your home directory and run it with qsub
#"qsub bondPricingBatch.sh" will start the job
#This script is intended to reserve 12 processors for Matlab worker processes
#In your batch script you can run "parpool('local',12)" to start 12 workers on a node
###################################################

#!/bin/bash
#PBS -N stats_calculator
#PBS -l nodes=1:ppn=5,mem=50g
#PBS -j oe
#PBS -V
#PBS -t 13-15

cd $PBS_O_WORKDIR

#Create a job specific temp directory
mkdir -p ~/BondPricing/bond-data/log_files/MERGE/$PBS_JOBID
export JULIAWORKDIR=~/BondPricing/bond-data/log_files/MERGE/$PBS_JOBID

# Load Python and Julia Modules
source ~/BondPricing/bond-data/module_loader.sh

# $PBS_NUM_PPN gives the number of processors to be used in each node.
# $PBS_ARRAYID gives the parameter combination # position of the coupon in the coupon grid -> Each batch gets one value of c!
echo $PBS_O_WORKDIR
echo $JULIAWORKDIR
echo $PBS_NODEFILE
echo $PBS_ARRAYID

# SYS Arguments:
# i. Number of Processors/Cores;
# ii. Parameter Combination (previously, $PBS_ARRAYID. Now: just enter combination number -> int);
job_num=$PBS_ARRAYID  # 1  # $PBS_ARRAYID

echo i. Number of processors/cores: $PBS_NUM_PPN
echo ii. Memory: $PBS_NUM_MEM
echo iii. Job Number: $job_num

julia -p $PBS_NUM_PPN stats_calculator.jl $job_num >> $JULIAWORKDIR/batch.log 2>&1
