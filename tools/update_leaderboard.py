#!/usr/bin/python3

"""Leader Board Data pre-processing

This simple program combines multiple leader board event files
(specified on the command line) and updates the existing JSON data
file with all unique events (based on the composite key).

The composite key is used as the top level dict key for purposes of 
sorting the events.

The current leader event data file (same as output file) is atomically
updated so it is safe to run this on a live system to update the 
leader data.
"""

import json
import os
import sys
import tempfile

leaderboard_file = "leaderboard.json"

unique_key_fields = "start_date athlete_name rank".split()

def make_composite_key(d: dict, keys: list) -> str:
    "Make a composite key (str) out of multiple dict values"
    vals = []
    for key in keys:
        vals.append(str(d.get(key, '')))
    return '+'.join(vals)

# Read all existing leaderboard events, on error clear list
try:
    with open(leaderboard_file, 'r') as f:
        data = json.load(f)
except:
    data = {}

# Read and add events from given files
for filename in sys.argv[1:]:
    with open(filename, 'r') as f:
        d = json.load(f)
        for entry in d['entries']:
            data[make_composite_key(entry, unique_key_fields)] = entry

# Write out all unique events and atomically replace data file
with tempfile.NamedTemporaryFile(mode = 'w', dir = '.', delete = False) as out:
    json.dump(data, out, indent=4, sort_keys=True)
    tmpname = out.name
os.replace(tmpname, leaderboard_file)


