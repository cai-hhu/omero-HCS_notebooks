# omero-HCS_notebooks
Utility notebook to interact with HCS data on OMERO.
These notebooks are used internally and should be reused with caution.

## Setup conda environment

```bash
conda create -n omero python=3.9 conda-forge::zeroc-ice==3.6.5 omero-py pip
conda activate myenv
pip install ezomero ipykernel
python -m ipykernel install --user --name=omero     # Setup the environment to use in jupyter 
```
