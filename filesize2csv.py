#!/bin/env python

import gzip
import zlib
import sys
import re
import os
import pdb
import time
import glob
from multiprocessing import Pool
import string
import json
from itertools import takewhile
from operator import eq

#@profile
def summarize_file_size(zipfile):

    # get proj name
    projid = zipfile.split('%')[-1].split('.')[0]

    print(f"Parsing {projid}")

    # init
    exts = {}
    years = {}
    locations = {}
    root = "<root>"
    ncdu_json = []
    pattern = re.compile('(\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (.+)$')

    try:

        # open file
        with gzip.open(zipfile, 'rb') as zf:

            # read it line by line
            i = 0
            t_prev = time.time()
            for line in zf:

                # convert to utf8
                try:
                    a = line.decode('utf-8').strip()
                except UnicodeDecodeError:
                    continue


                # find the pattern and pick out variables for readability
                match = pattern.match(a)

                # skip not matching lines
                if not match:
                    try:
                        print(f"Did not match: {a}")
                    except:
                        print("Did not match: non-UTF-8 chars in file, skipping line.")
                    continue

                size, user, group, umask, date, n1, n2, n3, n4, path = match.groups()

                # Create ncdu compatible list

                filetype = "d"
                if umask.split('/')[1].startswith('-'):
                    filetype = "f"

                if filetype == 'd' or int(size) > 10 * (1024**2):

                    dirs_name = path.rsplit('/',1)
                    dirs = dirs_name[0]
                    name = dirs_name[1]
                    pdb.set_trace()

                    ncdu_json.append("""{{"name":{},"asize":{},"dsize":{},"ino":{},"mtime":{},"type":"{}","dirs":{}}}\n""".format(
                            json.dumps(name),
                            size,
                            int(size) * 1024,
                            1,
                            1,
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


    # skip empty files
#    if len(exts) == 0:
#        print(f"Skipping empty file: {projid}")
#        return

    # write the csv files
    except EOFError:
        print(f"ERROR: EOFError while reading file {zipfil}")

    writecsv(f'{output_folder}/{projid}.exts.csv', exts, 'ext', re.compile('[\W]+'))
    writecsv(f'{output_folder}/{projid}.years.csv', years, 'year')
    writecsv(f'{output_folder}/{projid}.locations.csv', locations, 'location')


    # Stolen from https://github.com/wodny/ncdu-export
    # Write ncdu compatible output format for all files
    prev_dirs = []

    with gzip.open(f'{output_folder}/{projid}.ncdu.gz','wb') as csvfile:
        csvfile.write( """[1,0,{{"progname":"{0}","progver":"{1}","timestamp":{2}}}""".format( "filesize2csv.py", 1.0, int(time.time())).encode())
        for line in ncdu_json:
            obj = json.loads(line)
            dirs = obj["dirs"]
            if not isinstance(dirs, list):
                dirs = dirs.lstrip("/")
                dirs = dirs.split("/") if dirs else []
            etype = obj["type"]
            del obj["dirs"]
            del obj["type"]
            adjust_depth(dirs, prev_dirs, csvfile)
            if etype == "dir":
                csvfile.write(",\n[{}".format(json.dumps(obj)).encode())
                dirs.append(obj["name"])
            else:
                csvfile.write(",\n{}".format(json.dumps(obj)).encode())
            prev_dirs = dirs

        dirs = []
        adjust_depth(dirs, prev_dirs, csvfile)
        csvfile.write("]\n".encode())

    prev_dirs = []


    #with open(f'{output_folder}/{projid}.exts.csv', 'w', encoding='utf-8') as csvfile:
    #    csvfile.write("ext\tsize\tfreq\n")
    #    pattern = re.compile('[\W]+')
    #    for ext,row in exts.items():
    #        # sanitize extension names
    #        ext_sanitized = pattern.sub('', ext)
    #        csvfile.write(f"{ext_sanitized}\t{row[0]}\t{row[1]}\n")

    #with open(f'{output_folder}/{projid}.years.csv', 'w', encoding='utf-8') as csvfile:
    #    csvfile.write("year\tsize\tfreq\n")
    #    for year,row in years.items():
    #        csvfile.write(f"{year}\t{row[0]}\t{row[1]}\n")

    #with open(f'{output_folder}/{projid}.location.csv', 'w', encoding='utf-8') as csvfile:
    #    csvfile.write("backup\tsize\tfreq\n")
    #    for location,row in locations.items():
    #        csvfile.write(f"{location}\t{row[0]}\t{row[1]}\n")

    print(f"Finished {projid}")
    return [exts, years, locations]


def writecsv(filename, stats, colname, pattern=None):
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



# get options
target = sys.argv[1]
output_folder = sys.argv[2]

try:
    threads = int(sys.argv[3])
except:
    threads = 1

# create output folder if needed
os.makedirs(output_folder, exist_ok=True)

# if target is a dir, run for all gz files in it
if os.path.isdir(target):

    # get the list of files
    filelist = glob.glob(f"{target}/*.gz") + glob.glob(f"{target}/*.tmp")

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
    #multithreading = True
    multithreading = False
    if multithreading:
        pool = Pool(processes=threads)
        collected_stats = pool.map(summarize_file_size, filelist_dedup.values())
        pool.close()
        pool.join()
    else:
        collected_stats = [summarize_file_size(filelist_dedup['snic2021-22-1019'])]

    print(f"Parsing complete, making global statistics.")
    # loop through all saved exts and make a joint summary
    all_exts = {}
    all_years = {}
    all_locations = {}
    for exts, years, locations in collected_stats:

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


    writecsv(f"{output_folder}/all.exts.csv", all_exts, "ext", re.compile('[\W]+'))
    writecsv(f"{output_folder}/all.years.csv", all_years, "year")
    writecsv(f"{output_folder}/all.locations.csv", all_locations, "location")



    #with open(f'{output_folder}/all.csv', 'w', encoding='utf-8') as csvfile:

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
