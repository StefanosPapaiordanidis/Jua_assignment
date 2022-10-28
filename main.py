import argparse
from Functions import create_parquet


if __name__ == '__main__':
    print('Available arguments are: \n '
          '--timestamp_from (default: "2022-05-01 00:00:00") \n '
          '--timestamp_to (default: "2022-05-31 23:00:00") \n '
          '--h3_cell (default: "85754e67fffffff")')
    main_parser = argparse.ArgumentParser()
    main_subparsers = main_parser.add_subparsers(title='available commands', dest='main_command')

    create_parquet_parser = main_subparsers.add_parser('create_parquet',
                                                       help='Extracts data from an ERA5 nc file')
    create_parquet_parser.add_argument('--timestamp_from', help='Choose timestamp',
                                       required=False, default=None)
    create_parquet_parser.add_argument('--timestamp_to', help='Choose timestamp',
                                       required=False, default=None)
    create_parquet_parser.add_argument('--h3_cell', help='Choose h3 cell',
                                       required=False, default='85754e67fffffff')

    args = main_parser.parse_args()
    if args.main_command == 'create_parquet':
        create_parquet(args)
    else:
        print('Please try "python main.py create_parquet" in the command line. ')

