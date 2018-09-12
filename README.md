# Wind lidar signal reception as a hidden Markov process
Accurately classify lidar measurements as either wind or noise using a hidden markov random field model.

## Setup

To install the required R packages:

```R
install.packages(c('bookdown', 'rticles', 'reticulate', 'igraph', 'reshape2', 'ggplot2', 'scales', 'viridis'))
```

To install the required Python packages:

```sh
pip3 install numpy scipy xarray "git+https://github.com/ASRCsoft/raspPy.git@segmentation"
```
The code expects a data file, `cestm_roof80_20171003.nc` (not included in the github repository), in the root folder.

To use a non-system python (for example, if you are using Anaconda), write the path to the python binary in `python_path.txt` in the root folder.

## Generating the paper

Due to the massive amount of data being analyzed, some processing must be done before compiling the paper, with the results cached here. Run the database code (which is not on github yet -- but the results are) to get csv files with wind estimate validation results. Run `download_data.py` to get the netCDF file with data for example plots (which does not yet work).

Then knit lidar_noise.Rmd in RStudio.
