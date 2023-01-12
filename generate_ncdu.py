#!/bin/env python

import gzip
import zlib
#import zstd
import zstandard
import sys
import re
import io
import os
import pdb
import time
import glob
from multiprocessing import Pool
import string
import json
from itertools import takewhile
from datetime import datetime
from operator import eq
import numpy as np
import argparse
from collections import defaultdict
import time
from calendar import timegm
from pprint import pprint
import gc

# uncomment @profile to profile the code to see where time is spent.
# kernprof -l script_to_profile.py
# python -m line_profiler script_to_profile.py.lprof
#@profile
def summarize_file_size(archive_file):

    # get proj name
    projid = archive_file.split('%')[-1].split('.')[0]

    # check if result files already exist
    files_to_check = [  f'{root_dir}/{tmp_dir}/{projid}.exts.csv',
                        f'{root_dir}/{tmp_dir}/{projid}.years.csv',
                        f'{root_dir}/{tmp_dir}/{projid}.locations.csv',
                        f'{root_dir}/{ncdu_dir}/{projid}.ncdu.gz',
                    ]
    skip_file = False
    existing_files = []
    for file_to_check in files_to_check:
        if os.path.exists(file_to_check):
            existing_files.append(file_to_check)
            skip_file = True

    # skip this file
    if skip_file and not force:
        print(f'Skipping {projid}, output file(s) already exists: {", ".join(existing_files)}')
        # return empty dicts
        return [None, {}, {}, {}, {}]

    print(f"Parsing {projid}")

    # init
    exts = {}
    years = {}
    locations = {}
    factory = lambda: defaultdict(factory)
    users = factory()
    root = "<root>"
    ncdu_json = []
    pattern = re.compile('(\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (.+)$')

    gc.collect()
    
    try:

        # open file
        with open(archive_file, 'rb') as zf:

            # decompress the stream and wrap it as a text stream
            dctx = zstandard.ZstdDecompressor()
            reader = dctx.stream_reader(zf)
            text_stream = io.TextIOWrapper(reader, encoding='utf-8', errors='replace')

            # read it line by line
            i = 0
            t_prev = time.time()
            #inode_memory = set()
            #inode_lol_size = 1000
            #inode_lol = [ [] for i in range(inode_lol_size) ]
            for i, line in enumerate(text_stream):

                if i % 100000 == 0:

                    variable_names = vars().copy()
                    for variable_name in variable_names:
                        v_size = round(sys.getsizeof(vars()[variable_name])/1024**2)
                        if v_size >= 1:
                            print(f"{variable_name}\t{v_size:,} MiB".replace(',', ' '))

                # find the pattern and pick out variables for readability
                match = pattern.match(line)

                # skip not matching lines
                if not match:
                    try:
                        print(f"Did not match: {line}")
                    except:
                        print("Did not match: non-UTF-8 chars in file, skipping line.")
                    continue

                size, user, group, umask, date, inode, disk_space, n3, n4, path = match.groups()

                # skip or flag duplicate files based on inode id
                # wow, this really increased memory usage! limit to 10 threads now or a 128gb node will die
                #inode = int(inode)
                hardlink_flag = ""
                #if int(inode) in inode_memory:
                #if inode in inode_lol[ inode % inode_lol_size ]:
                #    hardlink_flag = ",\"hlnkc\":true"
                    
                    # skip them entierly, the row above only kind of fixes the problem.
                    # 1.3PiB with no fix -> 38TiB with above fix -> 20.6TiB with continue, and SAMS says 21.5TiB.. close enough
                    # need to investigate how ncdu handles inodes and hardlinks.
                #    continue 



                # remember inode id
                #inode_memory.add(int(inode))
                #inode_lol[ inode % inode_lol_size ].append(inode)

                # Create ncdu compatible list

                filetype = "d"
                if umask.split('/')[1].startswith('-'):
                    filetype = "f"

                #if filetype == 'd' or int(size) > 10 * (1024**2):
                #if True:

                dirs_name = path.rsplit('/',1)
                dirs = dirs_name[0]
                name = dirs_name[1]


                # convert timestamp of file to epoch time
                # using numpy for speed
                # https://www.geeksforgeeks.org/how-to-convert-numpy-datetime64-to-timestamp/
                np_dt = np.datetime64(date.split('.')[0])
                epoch_time = int((np_dt - np.datetime64('1970-01-01T00:00:00')) / np.timedelta64(1, 's'))

                ncdu_json.append("""{{"name":{},"asize":{},"dsize":{},"ino":{}{},"mtime":{},"type":"{}","dirs":{}}}\n""".format(
                        json.dumps(name),
                        size,
                        int(disk_space) * 1024,
                        inode,
                        hardlink_flag,
                        epoch_time,
                        "dir" if filetype == "d" else "file",
                        json.dumps(path and "{}{}".format(root, dirs) or root)))

                # check if dir
                if umask.split('/')[1].startswith('d'):
                    continue

                # get extension
                extension_split = os.path.basename(path)[1:].split('.') # remove first character if it is a hidden file starting with a dot


                # if there is no extension
                if len(extension_split) == 1:
                    ext = 'NA'

                # if there is an extension, grab the last one
                else:
                    ext = extension_split[-1]


                # save the stats
                try:
                    exts[ext][0] += int(size)
                    exts[ext][1] += 1

                # if it is the first time the extension is seen
                except KeyError:
                    exts[ext] = []
                    exts[ext].append(int(size))
                    exts[ext].append(1)

                # save user specific
                try:
                    users[user]['exts'][ext][0] += int(size)
                    users[user]['exts'][ext][1] += 1
                except TypeError:
                    users[user]['exts'][ext] = []
                    users[user]['exts'][ext].append(int(size))
                    users[user]['exts'][ext].append(1)


                # Save year of file
                file_year = date[0:4]

                # save the stats
                try:
                    years[file_year][0] += int(size)
                    years[file_year][1] += 1

                # if it is the first time the yearension is seen
                except KeyError:
                    years[file_year] = []
                    years[file_year].append(int(size))
                    years[file_year].append(1)

                # save user specific
                try:
                    users[user]['years'][file_year][0] += int(size)
                    users[user]['years'][file_year][1] += 1
                except TypeError:
                    users[user]['years'][file_year] = []
                    users[user]['years'][file_year].append(int(size))
                    users[user]['years'][file_year].append(1)



                # Check if backed up
                file_location = 'backup'
                if 'nobackup' in path:
                    file_location = 'nobackup'

                # save the stats
                try:
                    locations[file_location][0] += int(size)
                    locations[file_location][1] += 1

                # if it is the first time the file_locationsension is seen
                except KeyError:
                    locations[file_location] = []
                    locations[file_location].append(int(size))
                    locations[file_location].append(1)

                # save user specific
                try:
                    users[user]['locations'][file_location][0] += int(size)
                    users[user]['locations'][file_location][1] += 1
                except TypeError:
                    users[user]['locations'][file_location] = []
                    users[user]['locations'][file_location].append(int(size))
                    users[user]['locations'][file_location].append(1)

                #pdb.set_trace()



    # skip empty files
#    if len(exts) == 0:
#        print(f"Skipping empty file: {projid}")
#        return

    # skip empty projects

    # write the csv files
    except EOFError:
        print(f"ERROR: EOFError while reading file {zipfil}")

    writecsv(f'{root_dir}/{tmp_dir}/{projid}.exts.csv', exts, 'ext', re.compile('[\W]+'))
    writecsv(f'{root_dir}/{tmp_dir}/{projid}.years.csv', years, 'year')
    writecsv(f'{root_dir}/{tmp_dir}/{projid}.locations.csv', locations, 'location')


    # Stolen from https://github.com/wodny/ncdu-export
    # Write ncdu compatible output format for all files
    prev_dirs = []



#  _   _  ____ ____  _   _
# | \ | |/ ___|  _ \| | | |
# |  \| | |   | | | | | | |
# | |\  | |___| |_| | |_| |
# |_| \_|\____|____/ \___/
#
# NCDU

    with gzip.open(f'{root_dir}/{ncdu_dir}/{projid}.ncdu.gz','wb') as gzfile:
        gzfile.write( """[1,0,{{"progname":"{0}","progver":"{1}","timestamp":{2}}}""".format( "filesize2csv.py", 1.0, int(time.time())).encode())
        for line in ncdu_json:
            obj = json.loads(line)
            dirs = obj["dirs"]
            if not isinstance(dirs, list):
                dirs = dirs.lstrip("/")
                dirs = dirs.split("/") if dirs else []
            etype = obj["type"]
            del obj["dirs"]
            del obj["type"]
            adjust_depth(dirs, prev_dirs, gzfile)
            if etype == "dir":
                gzfile.write(",\n[{}".format(json.dumps(obj)).encode())
                dirs.append(obj["name"])
            else:
                gzfile.write(",\n{}".format(json.dumps(obj)).encode())
            prev_dirs = dirs

        dirs = []
        adjust_depth(dirs, prev_dirs, gzfile)
        gzfile.write("]\n".encode())

    prev_dirs = []



#  ____  _____ ____    _   _ ____  _____ ____
# |  _ \| ____|  _ \  | | | / ___|| ____|  _ \
# | |_) |  _| | |_) | | | | \___ \|  _| | |_) |
# |  __/| |___|  _ <  | |_| |___) | |___|  _ <
# |_|   |_____|_| \_\  \___/|____/|_____|_| \_\
#
# PER USER

    # write the user specific files
    for user in users.keys():

        # print each stat

        for stat_name,stat in users[user].items():

            # calculate user percentage of total project storage usage
            proj_tot_storage = sum( [ sizefreq[0] for sizefreq in locals()[stat_name].values() ])
            user_tot_storage = sum( [ sizefreq[0] for sizefreq in stat.values() ])

            try:
                # save as a zero filled 3-digit string of the number
                user_percent_storage = str(int(round(user_tot_storage / proj_tot_storage, 2) * 100 )).zfill(3)

            # if the project has no files proj_tot_storage will be 0, resulting in a DivideByZeroError
            except Exception:

                # set user percentage to zero and continue
                user_percent_storage = "000"

            
            # open a file handle
            with open(f'{root_dir}/{csv_dir}/{projid}.{user_percent_storage}.{user}.{stat_name}.csv', 'w', encoding='utf-8') as csvfile:


                # print metadata and header
                csvfile.write(f"Project:\t{projid}\nUser:\t\t{user}\n%tot\t\t{int(user_percent_storage)}%\n\n")
                csvfile.write(f"{stat_name}\tsize\tfreq\n")

                # print the stats in sorted order
                for row_name,row in sorted(stat.items(), key=lambda e: e[1][0], reverse=True):
                    csvfile.write(f"{row_name}\t{row[0]}\t{row[1]}\n")



# projid.json
# 
# projid.user.years
# projid.user.locations
# projid.user.exts
# 
# projid.years
# projid.locations
# projid.exts











#  ____   _____        ___   _ ____ _____ ____  _____    _    __  __
# |  _ \ / _ \ \      / / \ | / ___|_   _|  _ \| ____|  / \  |  \/  |
# | | | | | | \ \ /\ / /|  \| \___ \ | | | |_) |  _|   / _ \ | |\/| |
# | |_| | |_| |\ V  V / | |\  |___) || | |  _ <| |___ / ___ \| |  | |
# |____/ \___/  \_/\_/  |_| \_|____/ |_| |_| \_\_____/_/   \_\_|  |_|
#
# DOWNSTREAM

    # print the format to be used by downstream analysis, everything divided per user and in 1 file







    #with open(f'{root_dir}/{projid}.exts.csv', 'w', encoding='utf-8') as csvfile:
    #    csvfile.write("ext\tsize\tfreq\n")
    #    pattern = re.compile('[\W]+')
    #    for ext,row in exts.items():
    #        # sanitize extension names
    #        ext_sanitized = pattern.sub('', ext)
    #        csvfile.write(f"{ext_sanitized}\t{row[0]}\t{row[1]}\n")

    #with open(f'{root_dir}/{projid}.years.csv', 'w', encoding='utf-8') as csvfile:
    #    csvfile.write("year\tsize\tfreq\n")
    #    for year,row in years.items():
    #        csvfile.write(f"{year}\t{row[0]}\t{row[1]}\n")

    #with open(f'{root_dir}/{projid}.location.csv', 'w', encoding='utf-8') as csvfile:
    #    csvfile.write("backup\tsize\tfreq\n")
    #    for location,row in locations.items():
    #        csvfile.write(f"{location}\t{row[0]}\t{row[1]}\n")

    print(f"Finished {projid}")

    variable_names = vars().copy()
    for variable_name in variable_names:
        v_size = round(sys.getsizeof(vars()[variable_name])/1024**2)
        if v_size >= 1:
            print(f"{variable_name}\t{v_size:,} MiB".replace(',', ' '))

#    pdb.set_trace()
    
    # return all data, converting the defaultdict to regular dict in the process
    return [projid, exts, years, locations, defaultdict_to_regulardict(users)]


def writecsv(filename, stats, colname, pattern=None):
    """
    Wrapper function to write csv files.
    If a regex pattern is given it will find and remove it from the value before printing.
    Used mainly to remove any non-word ([\W]+) character from file endings.
    """
    with open(filename, 'w', encoding='utf-8') as csvfile:

        csvfile.write(f"{colname}\tsize\tfreq\n")

        for stat,row in stats.items():
            if pattern:
                # sanitize extension names
                stat = pattern.sub('', stat)

            csvfile.write(f"{stat}\t{row[0]}\t{row[1]}\n")




# Unflatten json to ncdu-format
def compare_dirs(dirs, prev_dirs):
    common_len = len(list(takewhile(lambda x: eq(*x), zip(dirs, prev_dirs))))
    closed = len(prev_dirs) - common_len
    opened = len(dirs) - common_len
    return closed, opened

def adjust_depth(dirs, prev_dirs, csvfile):
    closed, opened = compare_dirs(dirs, prev_dirs)
    if closed:
        csvfile.write(("]"*closed).encode())
    if opened:
        for opened_dir in dirs[-opened:]:
            csvfile.write(""",\n[{{"name":{},"asize":0,"dsize":0,"ino":0,"mtime":0}}""".format(json.dumps(opened_dir)).encode())


def defaultdict_to_regulardict(d):
    """
    Converts nested, or not, defaultdicts to regular dicts.
    """
    if isinstance(d, defaultdict):
        d = {k: defaultdict_to_regulardict(v) for k, v in d.items()}
    return d


# get options
parser = argparse.ArgumentParser(
                    prog = 'filesize2csv',
                    description = 'Converts the output from a find command to csv files to be used downstream.')
parser.add_argument('input', help="Input file or directory containing multiple files.")
parser.add_argument('output', help="Directory where output files and folders will be created.")
parser.add_argument('-t', '--threads', type=int, default=1, help="How many threads to use when parsing files. (Default 1)")
parser.add_argument('-f', '--force', action='store_true', help="Overwrite already existing output files.")
args = parser.parse_args()

# rename
target = args.input
root_dir = args.output
threads = args.threads
force = args.force

# set options
csv_dir = 'csv'
tmp_dir = 'tmp'
ncdu_dir = 'ncdu'

# create output folder if needed
os.makedirs(root_dir, exist_ok=True)
os.makedirs(f"{root_dir}/{csv_dir}", exist_ok=True)
os.makedirs(f"{root_dir}/{tmp_dir}", exist_ok=True)
os.makedirs(f"{root_dir}/{ncdu_dir}", exist_ok=True)

# if target is a dir, run for all gz/zst files in it
if os.path.isdir(target):

    # get the list of files
    #filelist = glob.glob(f"{target}/*.gz") + glob.glob(f"{target}/*.tmp")
    filelist = glob.glob(f"{target}/*.zst") + glob.glob(f"{target}/*.tmp")

    # sort file list by file size to begin processing the large files first
    filelist = sorted(filelist, key=os.path.getsize, reverse=True)

    # Remove duplicated projects
    filelist_dedup = {}

    for filename in filelist:
        projid = filename.split('%')[3].split('.')[0]
        filelist_dedup[projid] = filename

    # create a multithreading pool

    # distribute jobs
    print(f"Starting to parse.")
    multithreading = True
    #multithreading = False
    if multithreading:
        pool = Pool(processes=threads)
        #collected_stats = pool.map(summarize_file_size, filelist_dedup.values())
        collected_stats = pool.imap_unordered(summarize_file_size, filelist_dedup.values(), chunksize=1)
        pool.close()
        pool.join()
    else:
        collected_stats = [summarize_file_size(list(filelist_dedup.values())[0])]

    print(f"Parsing complete, making global statistics.")
#    pdb.set_trace()
    # loop through all saved exts and make a joint summary
    all_exts = {}
    all_years = {}
    all_locations = {}
    all_users = {}
    for projid, exts, years, locations, users in collected_stats:

        for ext, row in exts.items():
            # save the stats
            try:
                all_exts[ext][0] += row[0]
                all_exts[ext][1] += row[1]
            # if it is the first time the extension is seen
            except KeyError:
                all_exts[ext] = []
                all_exts[ext].append(row[0])
                all_exts[ext].append(row[1])

        for year, row in years.items():
            # save the stats
            try:
                all_years[year][0] += row[0]
                all_years[year][1] += row[1]
            # if it is the first time the yearension is seen
            except KeyError:
                all_years[year] = []
                all_years[year].append(row[0])
                all_years[year].append(row[1])

        for location, row in locations.items():
            # save the stats
            try:
                all_locations[location][0] += row[0]
                all_locations[location][1] += row[1]
            # if it is the first time the locationension is seen
            except KeyError:
                all_locations[location] = []
                all_locations[location].append(row[0])
                all_locations[location].append(row[1])


        if projid:
            all_users[projid] = users

    writecsv(f"{root_dir}/{tmp_dir}/all.exts.csv", all_exts, "ext", re.compile('[\W]+'))
    writecsv(f"{root_dir}/{tmp_dir}/all.years.csv", all_years, "year")
    writecsv(f"{root_dir}/{tmp_dir}/all.locations.csv", all_locations, "location")

    with open(f"{root_dir}/data_dump.json", 'w') as json_file:
        json_file.write(json.dumps(all_users))


    #with open(f'{root_dir}/{tmp_dir}/all.csv', 'w', encoding='utf-8') as csvfile:

    #    csvfile.write("ext\tsize\tfreq\n")
    #    pattern = re.compile('[\W]+')

    #    for ext,row in all_exts.items():

    #        # sanitize extension names
    #        ext_sanitized = pattern.sub('', ext)

    #        csvfile.write(f"{ext_sanitized}\t{row[0]}\t{row[1]}\n")
    print(f"Done.")
# else run just for that file
else:
    summarize_file_size(target)
