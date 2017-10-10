Program to find and list duplicate files in a root directory.

Usage: $0 <rootDir> [MAX_FILE_CHUNK]
	rootDir => The parent directory to scan the files and subdirectories.
	MAX_FILE_CHUNK => The largest byte chunk size in which a large file should be read to prevent OOM Error. [Default 1000000 bytes]

Output:
    Results are saved file filecompare_results.txt

Features:
1. Can traverse subdirectories, and symlinks (that can itself be a file or directory)
2. Can handle very large files by breaking them in chunks.

Limitations:
1. NOT TESTED FOR RECURSIVE SYMLINKS
2. Hard links resolver not implemented! The program was written and tested on windows. Unix inode resolution could not be tested.
3. This program does not implement parallelism and processes each result sequentially.
4. This program does not stop after execution completed. This is WIP- not implemented.
5. This program compares binary contents of files. Therefore does not account for EOL differences, spaces, case, etc. in text file.
6. Opportunities for general code cleanup, refactoring, unit tests, etc.
7. Not tested (possible duplicates reported) if multiple symlinks point to same file or directory
8. Autorecovery from failures (restart long job from checkpoints) not implemented
9. File access/ read errors are logged and then ignored, no cumulative reporting of errored files."# filecompare-bulk-serial" 
