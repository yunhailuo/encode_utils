#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Post files, get credentials but no real upload to AWS
"""

import argparse
import binascii
import os

import encode_utils.connection as euc


def get_parser():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        '-m',
        '--dcc-mode',
        required=True,
        help='The ENCODE Portal site ("prod" or "dev", or an explicit '
             'host name, i.e. "demo.encodedcc.org") to connect to.'
    )
    parser.add_argument(
        '-r',
        '--run',
        action='store_true',
        help='Perform real post to the target machine.'
    )
    parser.add_argument(
        '-n',
        '--num-files',
        required=True,
        type=int,
        help='Number of fake files to post.'
    )
    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()
    conn_search = euc.Connection(args.dcc_mode)
    experiments = conn_search.search(
        search_args=[
            ('type', 'Experiment'),
            ('replicates', '*'),
            ('field', 'replicates.@id')
        ],
        limit='all'
    )
    experiment_count = len(experiments)
    file_template = {
        '_profile': 'file',
        'lab': '/labs/j-michael-cherry/',
        'award': '/awards/U24HG009397/',
        'dataset': '',
        'file_size': 1,
        'md5sum': '',
        'file_format': 'fastq',
        'output_type': 'reads',
        'replicate': '',
        'run_type': 'paired-ended',
        'paired_end': '1',
        'platform': '/platforms/OBI:0002630/',
        'read_length': 16,
        'submitted_file_name': '/path/not/exist.fastq.gz',
    }
    conn_post = euc.Connection(args.dcc_mode, not args.run, submission=True)
    for i in range(args.num_files):
        selected_index = i % experiment_count
        file_template['dataset'] = experiments[selected_index]['@id']
        file_template['md5sum'] = binascii.hexlify(os.urandom(16)).decode()
        exp_reps = experiments[selected_index]['replicates']
        file_template['replicate'] = exp_reps[i % len(exp_reps)]['@id']
        conn_post.post(file_template, require_aliases=False)


if __name__ == '__main__':
    main()
