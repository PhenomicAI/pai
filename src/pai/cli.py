import argparse
from .embed import PaiEmbeddings


def app():
    parser = argparse.ArgumentParser("PAI (Phenomic AI) CLI")
    sub_parser = parser.add_subparsers(dest="command")

    embed = sub_parser.add_parser("embed")
    embed.add_argument('--tmp-dir', required=True, type=str, help="")
    embed.add_argument('--h5ad-path', required=True, type=str, help="")
    embed.add_argument('--tissue-organ', required=True, type=str, help="")

    args = parser.parse_args()

    if args.command == "embed":
        pai_embeddings = PaiEmbeddings(args.tmp_dir)
        pai_embeddings.inference(args.h5ad_path, args.tissue_organ)


if __name__ == "__main__":
    app()