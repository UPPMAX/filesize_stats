# File size statistics

Every once in a while a find command is run by one of the system administrators (takes about a month to run for all projects)

```bash
find "$DIR" -printf "%s %u %g %m/%M %CY-%Cm-%CdT%CH:%CM:%CS %i %k - - %p\n" 2>"$ERROR" | zstd -9 >"$FTMP"
```

that will create a `zstd` file for each project at UPPMAX. The file will contain a row for each file and folder in that project's folder,

```bash
57653 username snic2022-22-999 664/-rw-rw-r-- 2022-10-26T02:02:20.0000000000 1675347254266851430 64 - - /crex/proj/snic2022-22-999/nobackup/download-sra/nf/.nextflow.log.1
```

These file are the raw data for calculating various statistics. The analysis is divided into 2 parts, where the first part is Python based and will read the `zst` files and summarize them into much smaller `csv` files. The 2nd part is an R script that will make plots based on the `csv` files.




