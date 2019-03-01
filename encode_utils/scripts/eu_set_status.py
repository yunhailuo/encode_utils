#!/usr/bin/env python3
# -*- coding: utf-8 -*-

###
# © 2018 The Board of Trustees of the Leland Stanford Junior University
# Nathaniel Watson
# nathankw@stanford.edu
###

"""
For the provided DCC record identifiers, change (patch) each record's status to
the provided status. This is for DCC admin only since the set_status endpoint
is admin only.
"""

import argparse
import inspect
import json

import encode_utils.connection as euc
import encode_utils.utils as euu
from encode_utils.parent_argparser import dcc_login_parser
from encode_utils.MetaDataRegistration.eu_register import RECORD_ID_FIELD


def get_parser():
    parser = argparse.ArgumentParser(
        description=__doc__,
        parents=[dcc_login_parser],
        formatter_class=argparse.RawTextHelpFormatter)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-r", "--records", nargs="+", help="""
                       One or more DCC record identifiers. -s/--status is
                       required if this option is used.""")
    group.add_argument("-i", "--infile", help="""
                       An input file containing one or more DCC record
                       identifiers, one per line. Empty lines and lines
                       starting with '#' are skipped. """)
    parser.add_argument("-s", "--status", help="""
                        The new status for records specified. Please note that
                        if this option is used, it will override any settings
                        in the input file.""")
    parser.add_argument("-d", "--dry-run", action="store_true", help="""
                        Set this option to enable the dry-run feature, such
                        that no modifications are performed on the ENCODE
                        Portal. This is useful if you'd like to ensure the
                        validity of your status changes.""")
    parser.add_argument("-v", "--verbose", action="store_true", help="""
                        Print out detailed status changes happened or
                        considered.""")
    parser.add_argument("--block-children", action="store_true", help="""
                        Only change status for objects specified with ENCODE ID
                        and do not consider related objects.""")
    parser.add_argument("--force-audit", action="store_true", help="""
                        Tell the ENCODE API to ignore audits on records when
                        changing status. Please note that if this option is
                        used, it will override any settings in the input
                        file.""")
    parser.add_argument("--force-transition", action="store_true", help="""
                        Overide the transition table of ENCODE API and force
                        the API to make whatever status changes as defined
                        here. Please note that if this option is used, it will
                        override any settings in the input file.""")
    return parser


def create_set_status_payloads(infile):
    header = []
    with open(infile) as f:
        for line in f:
            line = line.strip("\n")
            if not line.strip() or line.startswith("#"):
                continue
            values = line.split('\t')
            if not header:
                header = values
                continue
            payload = dict(zip(header, values))
            if not payload.get(RECORD_ID_FIELD, ''):
                msg = (
                    "Missing a record identifier in input file for status "
                    "change at line:\n\t{}\nPlease use '{}' field to specify "
                    "a record accession/uuid/@id."
                )
                raise ValueError(msg.format(line, RECORD_ID_FIELD))
            yield payload


def main():
    parser = get_parser()
    args = parser.parse_args()
    if ('records' in args) and ('status' not in args):
        parser.error('-s/--status is required if -r/--records option is used.')
    rec_ids = args.records
    infile = args.infile
    dry_run = bool(args.dry_run)
    priority_payload = {}
    if args.status:
        priority_payload['status'] = args.status
    if args.force_audit:
        priority_payload['force_audit'] = 'y'
    if args.force_transition:
        priority_payload['force_transition'] = 'y'

    # Connect to the Portal
    dcc_mode = args.dcc_mode
    if dcc_mode:
        conn = euc.Connection(dcc_mode, dry_run=dry_run)
    else:
        # Default dcc_mode taken from environment variable DCC_MODE.
        conn = euc.Connection(dry_run=dry_run)
    if not rec_ids:
        # Then get them from input file
        gen = create_set_status_payloads(infile)
    else:
        gen = ({RECORD_ID_FIELD: rec_id} for rec_id in rec_ids)
    for payload in gen:
        unknown_params = [
            param for param in payload
            if param not in inspect.getfullargspec(conn.set_status)[0]
        ]
        if unknown_params:
            msg = "Unknown parameters '{}' for set_status endpoint."
            raise ValueError(msg.format(str(unknown_params)))
        payload.update(priority_payload)
        try:
            res = conn.set_status(**payload)
            if args.verbose:
                print(euu.print_format_dict(res))
        except Exception as e:
            if e.response.status_code == 422:  # Unprocessable Entity
                # Then likely this property is not defined for this record.
                text = json.loads(e.response.text)
                print("Can't PATCH record: {}".format(text["errors"]))


if __name__ == "__main__":
    main()
