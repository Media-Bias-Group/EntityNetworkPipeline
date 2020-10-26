import argparse
from entitynetwork.helper_classes import info
from entitynetwork.helper_classes import model_downloader


def print_paths(args):
    info.print_info()


def download(args):

    if model_downloader.ModelDownloader().model_exists(model_name=args.model_name,
                                                       model_data_path=args.model_path):
        while True:
            res = input("Model exists, overwrite? (y/n) ")
            if res == 'y' or res == 'Y':
                force_download(args)
                break
            elif res == 'n' or res == 'N':
                break
    else:
        force_download(args)


def force_download(args):
    path = model_downloader.ModelDownloader().download(model_name=args.model_name,
                                                       model_data_path=args.model_path,
                                                       url=args.model_url)
    print("Downloaded model to " + path)


def start():
    # top level parser
    parser = argparse.ArgumentParser(description='NewsRelations')
    subparsers = parser.add_subparsers(title='valid sub-commands')

    # info parser
    parser_d = subparsers.add_parser('paths', help='view NewsRelations local paths')
    parser_d.set_defaults(func=print_paths)

    # download parser
    parser_d = subparsers.add_parser('download', help='downloader for (default) databases')
    parser_d.add_argument('-n', '--model_name', nargs='?', help='target name of the model', default=None)
    parser_d.add_argument('-p', '--model_path', nargs='?', help='target path of the model', default=None)
    parser_d.add_argument('-u', '--model_url', nargs='?', help='source url of the model', default=None)
    parser_d.set_defaults(func=download)

    args = parser.parse_args()
    if "func" not in args:
        parser.print_help()
    else:
        args.func(args)


if __name__ == "__main__":
    start()