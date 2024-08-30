import argparse
from embed import PaiEmbeddings

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--results-dir', default="tmp/results/", type=str, help="")
    parser.add_argument('--h5ad-path', required=True, type=str, help="")
    parser.add_argument('--tissue-organ', required=True, type=str, help="")

    args = parser.parse_args()

    pai_embeddings = PaiEmbeddings(args.results_dir)
    pai_embeddings.inference(args.h5ad_path, args.tissue_organ)
