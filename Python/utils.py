import argparse
import os
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd


def output_clairvoyance(save_dir, sta, dyn, outc, attrition, task_type="static"):
    os.makedirs(save_dir, exist_ok=True)
    dyn.melt(id_vars=['stay_id', 'time'])
    if (task_type == "static"):
        pd.join(outc, sta)
    else:
        pd.join(outc, dyn)
    pq.write_table(pa.Table.from_pandas(dyn), os.path.join(save_dir, 'dyn.parquet'))
    pq.write_table(pa.Table.from_pandas(sta), os.path.join(save_dir, 'sta.parquet'))


def output_yaib(save_dir, sta, dyn, outc, attrition):
    os.makedirs(save_dir, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(outc), os.path.join(save_dir, 'outc.parquet'))
    pq.write_table(pa.Table.from_pandas(dyn), os.path.join(save_dir, 'dyn.parquet'))
    pq.write_table(pa.Table.from_pandas(sta), os.path.join(save_dir, 'sta.parquet'))

    attrition.to_csv(os.path.join(save_dir, 'attrition.csv'))


def make_argument_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--src', default='mimic_demo', help='name of datasource',
                        choices=['aumc', 'eicu', 'eicu_demo', 'hirid', 'mimic', 'mimic_demo', 'miiv'])
    parser.add_argument('--out_dir', default='../data/los', help='path where to store extracted data',
                        choices=['aumc', 'eicu', 'eicu_demo', 'hirid', 'mimic', 'mimic_demo', 'miiv'])
    parser.add_argument('--out_type', default='yaib', help='output format', choices=['yaib', 'clairvoyance'])
    return parser
