package example.kv.filecompare;

import example.kv.filecompare.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class FileCompareBulkSerial {

    private static final Logger LOG = LoggerFactory.getLogger(FileCompareBulkSerial.class);

    // the output file location relative to the application run time location
    private static final String OUTPUT_FILE = "filecompare_results.txt";

    private final FileUtils fileUtils = new FileUtils();

    // entry point of the local program
    public static void main(String[] args) {
        String usage = "$0 <rootDir> [MAX_FILE_CHUNK]\n\t rootDir => The parent directory to scan the files and subdirectories.\n\tMAX_FILE_CHUNK => The largest byte chunk size in which a large file should be read to prevent OOM Error. [Default 1000000 bytes]";
        FileCompareBulkSerial mainClass = new FileCompareBulkSerial();
        if (args.length <1 ) {
            LOG.error("Invalid args. Usage: {}", usage);
            System.exit(-1);
        }
        if (args.length > 1) {
            try {
                int maxFileChunk = Integer.parseInt(args[1]);
                FileUtils.MAX_FILE_SIZE = maxFileChunk;
            } catch (NumberFormatException e) {
                LOG.error("Invalid args. Usage: {}. Args {}", usage, args, e);
                System.exit(-1);
            }
        }
        mainClass.start(args[0]);
    }

    private void start(String rootDirName) {

        Map<String,Long> filesMap = new HashMap<>();

        Map<Long, List<String>> potentialDuplicateFilesMap = new ConcurrentHashMap<>();

        Map<String, List<String>> confirmedHashFilesMap = new ConcurrentHashMap<>();

        // STEP 1: Read Root Directory recursively and get files list
        listFilesInDir(rootDirName, filesMap);

        // STEP 2: Identify Potential duplicates based on file Size and add to new Map
        compareAndAddPotentialDuplicates(filesMap, potentialDuplicateFilesMap);

        // STEP 3: Compare file contents hash to add confirmed duplicates
        compareAndAddHashDuplicates(potentialDuplicateFilesMap, confirmedHashFilesMap);

        // STEP 4: Save output results to a file
        saveOutputMain(confirmedHashFilesMap);

        LOG.info("Success. process complete");
    }

    private void listFilesInDir(String dirName, Map<String, Long> filesMap) {
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(dirName).toRealPath())) {
            for (Path path : directoryStream) {
                // First find the referenced File object for symbolic links
                if (Files.isSymbolicLink(path) ) {
                    Optional<Path> optionalPath = getValidPathForSymLink(path);
                    if (optionalPath.isPresent()) {
                        path = optionalPath.get();
                    } // else symlink was invalid and shall be dropped
                //} else if (fileUtils.isHardLink(path)) {

                }
                // Process if path is a directory
                if (Files.isDirectory(path)) {
                    // recursively list dir. TODO: change to executors later
                    listFilesInDir(path.toString(), filesMap);
                } else {
                    try {
                        long size = Files.size(path);
                        filesMap.put(path.toString(), size);
                    } catch (IOException e) {
                        LOG.error("IOException getting file size. ignored " + path.toString(), e);
                    }
                }
            }
        } catch (IOException e) {
            LOG.error("IOException listing files in " + dirName, e);
        }
    }

    private Optional<Path> getValidPathForSymLink(Path path) {
        try {
            Path p = Files.readSymbolicLink(path);
            if (Files.isSymbolicLink(p)) {
                Optional<Path> optionalPath = getValidPathForSymLink(p);
                if (optionalPath.isPresent()) {
                    p = optionalPath.get();
                }
            }
            if (Files.exists(p)) {
                return Optional.of(p);
            }
        } catch (IOException e) {
            LOG.error("IOException reading symlink " + path.toString(), e);
        }
        return Optional.empty();
    }

    private void compareAndAddPotentialDuplicates(Map<String, Long> filesMap, Map<Long,List<String>> potentialDuplicateFilesMap) {
        filesMap.entrySet().stream().forEach(entry -> addToMap1(entry, potentialDuplicateFilesMap));
        // After after complete, remove entries with ONLY ONE file
        potentialDuplicateFilesMap.entrySet().stream().forEach(entry -> keepSameSizedFiles1(entry, potentialDuplicateFilesMap));
    }

    private void addToMap1(Map.Entry<String, Long> entry, Map<Long,List<String>> potentialDuplicateFilesMap) {
        long size = entry.getValue();
        List<String> filesList = potentialDuplicateFilesMap.get(size);
        if (filesList == null) {
            filesList = new ArrayList<>();
        }
        filesList.add(entry.getKey());
        potentialDuplicateFilesMap.put(size, filesList);
    }

    private void keepSameSizedFiles1(Map.Entry<Long, List<String>> entry, Map<Long,List<String>> potentialDuplicateFilesMap) {
        if (entry.getValue().size() < 2) {
            potentialDuplicateFilesMap.remove(entry.getKey());
        }
    }

    private void compareAndAddHashDuplicates(Map<Long,List<String>> potentialDuplicateFilesMap, Map<String,List<String>> confirmedHashFilesList) {
        potentialDuplicateFilesMap.entrySet().stream().forEach(entry -> addToMap2(entry, potentialDuplicateFilesMap, confirmedHashFilesList));
        // All files added to map - remove non-duplicates
        confirmedHashFilesList.entrySet().stream().forEach(entry2 -> keepSameSizedFiles2(entry2, confirmedHashFilesList));
    }

    private void addToMap2(Map.Entry<Long, List<String>> entry, Map<Long, List<String>> potentialDuplicateFilesMap, Map<String, List<String>> confirmedHashFilesMap) {
        for (String fileName : entry.getValue()) {
            String md5;
            // Bypass MD5 checks if file size = 0
            if (entry.getKey() == 0) {
                md5 = "0";
            } else if (entry.getKey() <= FileUtils.MAX_FILE_SIZE) {
                md5 = fileUtils.getMD5ForFile(fileName);
            } else {
                md5 = fileUtils.getMD5ForFileByParts(fileName, entry.getKey());
            }
            String key = entry.getKey() + "-" + md5;
            addFiletoConfirmedMap(key, fileName, confirmedHashFilesMap);
        }
    }

    private void addFiletoConfirmedMap(String key, String fileName, Map<String, List<String>> confirmedHashFilesMap) {
        List<String> files = confirmedHashFilesMap.get(key);
        if (files == null) {
            files = new ArrayList<>();
        }
        files.add(fileName);
        confirmedHashFilesMap.put(key, files);

    }

    private void keepSameSizedFiles2(Map.Entry<String, List<String>> entry, Map<String,List<String>> confirmedHashFilesList) {
        if (entry.getValue().size() < 2) {
            confirmedHashFilesList.remove(entry.getKey());
        }
    }

    private void saveOutputMain(Map<String, List<String>> confirmedHashFilesMap) {
        StringBuilder sb = new StringBuilder();
        sb.append("Count of items with duplicates:").append(confirmedHashFilesMap.size()).append("\n");
        confirmedHashFilesMap.entrySet().forEach(entry -> sb.append("Group:").append(entry.getKey()).append(":").append(printFiles(entry.getValue())).append("\n"));

        Path file = Paths.get(OUTPUT_FILE);
        try {
            Files.write(file, sb.toString().getBytes());
        } catch (IOException e) {
            LOG.error("IOException writing output file ", e);
        }
        LOG.info("Successfully saved output to {}", OUTPUT_FILE);
    }

    private char[] printFiles(List<String> fileNames) {
        StringBuilder sb = new StringBuilder();
        for (String fileName : fileNames) {
            sb.append(":").append(fileName);
        }
        return sb.toString().toCharArray();
    }

}
