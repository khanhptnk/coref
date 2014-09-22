"""
Usage: python create_onfs.py -c create_onfs.conf
"""

from __future__ import with_statement

import on
import on.common
import on.common.util
from on.common.util import register_config

@register_config("out", "out_dir", required=True, section_required=True)
def create_onfs():
    """ Reads a configuration from config_fname to decide what files
    to load to the database.
    """
    config = on.common.util.load_options(positional_args=False)

    a_ontonotes = on.ontonotes(config)

    for a_subcorpus in a_ontonotes:
        print "Loading", a_subcorpus.id
        a_subcorpus["parse"].dump_onf(a_cursor=None, out_dir=config["out", "out_dir"])

if __name__ == "__main__":
    create_onfs()
