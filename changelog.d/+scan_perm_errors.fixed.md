Fix LocalFolder.all_files(..) erroring out if one of the non-excluded directories is not readable by the user running the scan.
Warning is added to ProgressReport instead as other file access errors are.
