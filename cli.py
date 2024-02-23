import argparse
import logging
import os
import re
import readline
from datetime import datetime
from pprint import pprint

import pandas as pd


def load_dataset(dataset_path):
    if dataset_path.endswith('.csv'):
        return pd.read_csv(dataset_path)
    elif dataset_path.endswith('.json'):
        return pd.read_json(dataset_path)
    elif dataset_path.endswith('.jsonl'):
        return pd.read_json(dataset_path, lines=True)
    elif dataset_path.endswith('.xlsx'):
        return pd.read_excel(dataset_path)
    else:
        raise NotImplementedError("We only support .csv/.json/.xlsx for now")


def set_logger(log_dir: str):
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_id = datetime.now().strftime('%H_%M_%d_%m_%Y')
    log_file = f'{log_dir}/{log_id}.log'
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                        level=logging.INFO,
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename=log_file,
                        filemode='w')


class CommandParser:

    def __init__(self, dataset_df):
        self.df = dataset_df

    def parse_command(self, command):
        if command == 'show':
            self.list_features()
            return
        elif command == 'head':
            result = self.show_rows_range(0, 5)
        elif command == 'tail':
            result = self.show_rows_range(-5, -1)
        elif command == 'len':
            result = len(self.df)
        elif re.match(r'\d+', command):
            row_idx = int(re.findall(r'\d+', command)[0])
            result = self.show_row(row_idx)
        elif re.match(r'\[\d+\]', command):
            row_idx = int(re.findall(r'\[(\d+)\]', command)[0])
            result = self.show_row(row_idx)
        elif re.match(r'\[\d+\]\.\w+', command):
            row_idx, feature = re.findall(r'\[(\d+)\]\.(\w+)', command)[0]
            result = self.show_feature_value(int(row_idx), feature)
        elif re.match(r'\[\d+:\d+\]\.\w+', command):
            start_idx, end_idx, feature = re.findall(r'\[(\d+):(\d+)\]\.(\w+)',
                                                     command)[0]
            result = self.show_rows_range(int(start_idx), int(end_idx))
            result = [r[feature] for _, r in result.iterrows()]
        elif re.match(r'\[\d+:\d+\]', command):
            start_idx, end_idx = map(
                int,
                re.findall(r'\[(\d+):(\d+)\]', command)[0])
            result = self.show_rows_range(start_idx, end_idx)
        elif re.match(r'filter\.\w+=.+', command):
            feature, value = re.findall(r'filter\.(\w+)=(.+)', command)[0]
            result = self.filter_rows(feature, value)
        #... (可以按照這樣的模式添加其他的指令解析和功能)
        else:
            print('Unknown command')
            return

        if not isinstance(result, pd.DataFrame):
            pprint(result)
            logging.info(result)
        elif len(result.shape) > 1:
            for _, item in result.iterrows():
                pprint(item)
                print('--')
                logging.info(item)
        else:
            pprint(result)
            logging.info(result.to_string())

    def list_features(self):
        print(self.df.columns.tolist())

    def show_row(self, row_idx):
        return self.df.iloc[row_idx]

    def show_feature_value(self, row_idx, feature):
        return self.df.at[row_idx, feature]

    def show_rows_range(self, start_idx, end_idx):
        return self.df.iloc[start_idx:end_idx]

    def filter_rows(self, feature, value):
        return self.df[self.df[feature] == value]


def main(args):
    print('Welcome to the interactive shell!')
    print('Type "exit()" to exit the shell')
    print('Load dataset from:', args.dataset_path)

    dataset_df = load_dataset(args.dataset_path)
    print('Dataset loaded successfully! Dataset shape:', dataset_df.shape)

    tool = CommandParser(dataset_df)

    if args.log:
        set_logger(args.log_dir)

    # start interactive shell
    while True:
        command = input('> ')
        logging.info(f'> {command}')

        if command == 'exit()':
            break

        try:
            tool.parse_command(command)
        except Exception as e:
            logging.error(e)
            print(e)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--dataset_path', type=str, required=True)
    parser.add_argument('--log',
                        action=argparse.BooleanOptionalAction,
                        default=False)
    parser.add_argument('--log_dir', type=str, default='./logs')
    args = parser.parse_args()

    main(args)
