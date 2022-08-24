#!/bin/env python

import gzip
import sys
import re
import os
import pdb
import time
import glob
from multiprocessing import Pool
import string

#@profile
def summarize_file_size(zipfile):

    # get proj name
    projid = zipfile.split('%')[-1].split('.')[0]

    print(f"Parsing {projid}")

    # init
    exts = {}
    years = {}
    locations = {}
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

    #            # print progress
    #            if i % 100000 == 0:
    #                t_now = time.time()
    #                t_diff = t_now - t_prev
    #                print(f"{projid}\t{i}\t{t_diff}")
    #                t_prev = t_now
    #            i+=1

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
    pool = Pool(processes=threads)

    # distribute jobs
    print(f"Starting to parse.")
    collected_stats = pool.map(summarize_file_size, filelist_dedup.values())
    pool.close()
    pool.join()

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
