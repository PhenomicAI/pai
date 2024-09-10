# phenomic-ai

A CLI package which facilitates computational biologists with a single cell RNA embedding tool

## Installation

`$ pip install phenomic-ai`

## Usage (examples)

`$ pai embed --tmp-dir /home/ubuntu/tmp/pai/embed --h5ad-path /home/ubuntu/tmp/pai/embed/adatas/anndata.h5ad --tissue-organ adipose`

Or

`$ python3 -m pai embed --tmp-dir /home/ubuntu/tmp/pai/embed --h5ad-path /home/ubuntu/tmp/pai/embed/adatas/anndata.h5ad --tissue-organ adipose`

Commands:

- `pai` main command
- `embed` sub-command invoking the embedding tool

Parameters:

- `--tmp-dir` (temporary direcetory) parameter is the root output directory where the downloaded zip files (zips/) and unzipped directories (results/) will be output
- `--h5ad-path` (h5ad path) parameter is the path to the single cell RNA .h5ad file intended to be uploaded and embeded
- `--tissue-organ` (tissue/organ) parameter specifies the tissue/organ associated wrt. the single cells

## Support

Email: sctx@phenomic.ai
