package example.kv.filecompare.util;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileUtils {
    private static final Logger LOG = LoggerFactory.getLogger(FileUtils.class);

    public static int MAX_FILE_SIZE = 1000000; // 1 MB max file chunk to read at a time

    public String getMD5ForFile(String fileName) {
        try (InputStream is = Files.newInputStream(Paths.get(fileName));
        ) {
            String md5 = DigestUtils.md5Hex(is);
            return md5;
        } catch (IOException e) {
            LOG.error("IOException reading file stream " + fileName, e);
        }
        return null;
    }

    public String getMD5ForFileByParts(String fileName, long size) {
        StringBuilder sb = new StringBuilder();
        try (InputStream is = Files.newInputStream(Paths.get(fileName));
             BufferedInputStream bis=new BufferedInputStream(is);
        ) {
            for (int offset =0 ; offset <= size; offset+= MAX_FILE_SIZE) {
                int bytesToRead = (int) ( (size-offset) >= MAX_FILE_SIZE? MAX_FILE_SIZE : (size-offset) );
                byte[] bytes = new byte[bytesToRead];
                try {
                    int bytesRead = bis.read(bytes);
                } catch (IOException e) {
                    LOG.error("IOException reading file bytes. Dropped.", e);
                }
                String md5 = DigestUtils.md5Hex(bytes);
                sb.append(md5);
            }
            String md5 = DigestUtils.md5Hex(sb.toString().getBytes());
            return md5;
        } catch (IOException e) {
            LOG.error("IOException reading file stream " + fileName, e);
        }
        return null;
    }


    public boolean isHardLink(Path path) {
        try {
            int inode = (Integer) Files.getAttribute( Paths.get(path.toString()), "unix:nlink" );
        } catch (IOException e) {
            LOG.error("IOException checking hard link for file {}", path.toString(), e);
        }
        return false;
    }
}
