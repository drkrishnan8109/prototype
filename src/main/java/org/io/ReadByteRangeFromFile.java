package org.io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class ReadByteRangeFromFile {

    public byte[] readByteRangeFromFile(String filePath, int startByteOffset, int length) throws IOException {
        RandomAccessFile file = null;
        byte[] byteArray = new byte[length];
        try {
            //Create a file stream to read from
            file = new RandomAccessFile(filePath, "r");
            int bytelength = file.read(byteArray, startByteOffset, length);

            if(bytelength <length) {
                // Dont send byteArray with empty bytes at end. Use System.arraycopy and
                // shorten the bytearray and make it compact before returning
            }

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } finally {
            file.close();
        }
        return byteArray;
    }

}
