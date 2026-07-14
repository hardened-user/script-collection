#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------------------------------------------------
import argparse
import sys
import traceback

from cryptography.fernet import Fernet, InvalidToken


def main():
    # __________________________________________________________________________
    # command-line options, arguments
    try:
        parser = argparse.ArgumentParser(
            description='Decrypt a Fernet token.',
            epilog='example:\n\t%(prog)s fernet:gAAAAA...  zJ2h9x...==',
            formatter_class=argparse.RawDescriptionHelpFormatter
        )
        parser.add_argument('token', action='store', type=str,
                            metavar='<TOKEN>', help="fernet token to decrypt")
        parser.add_argument('key', action='store', type=str,
                            metavar='<FERNET_KEY>', help="fernet key (32 url-safe base64-encoded bytes)")
        parser.add_argument('-q', '--quiet', action='store_true',
                            help="print only the decrypted result, without decoration")
        args = parser.parse_args()
    except SystemExit:
        return False
    # __________________________________________________________________________
    try:
        f = Fernet(args.key.encode())
        token = args.token.encode().split(b'fernet:', 1)[-1]
        plaintext = f.decrypt(token).decode()
        if args.quiet:
            print(plaintext, flush=True)
        else:
            width = max((len(line) for line in plaintext.splitlines()), default=0)
            dashes = max(width - 2, 0)
            print("┌{}┐\n".format('─' * dashes), flush=True)
            print(plaintext, flush=True)
            print("\n└{}┘".format('─' * dashes), flush=True)
    except InvalidToken:
        print("[EE] Invalid key or corrupted token.", flush=True)
    except Exception as err:
        print("[!!] Exception :: {}\n{}".format(err, "".join(traceback.format_exc(limit=1))), flush=True)
        return False
    # __________________________________________________________________________
    return True


# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
if __name__ == '__main__':
    exit_status = main()
    # __________________________________________________________________________
    sys.exit(not exit_status)
