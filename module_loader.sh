# Define Modules
pythonMod=python/intelpython3
juliaPath=/home/artur/BondPricing/.julia
juliaVersion=julia-1.5.3

# Load Python and Julia Modules
module load $pythonMod 
export PATH=$juliaPath/$juliaVersion/bin:$PATH
export LD_LIBRARY_PATH=$juliaPath/$juliaVersion/bin:$LD_LIBRARY_PATH
