import argparse
from embed import PaiEmbeddings

if __name__ == "__main__":

    parser = argparse.ArgumentParser("PAI (Phenomic AI) CLI")
    sub_parser = parser.add_subparsers(dest="command")

    embed = sub_parser.add_parser("embed")
    embed.add_argument('--results-dir', default="tmp/results/", type=str, help="")
    embed.add_argument('--h5ad-path', required=True, type=str, help="")
    embed.add_argument('--tissue-organ', required=True, type=str, help="")

    args = parser.parse_args()

    if args.command == "embed":
        pai_embeddings = PaiEmbeddings(args.results_dir)
        pai_embeddings.inference(args.h5ad_path, args.tissue_organ)
