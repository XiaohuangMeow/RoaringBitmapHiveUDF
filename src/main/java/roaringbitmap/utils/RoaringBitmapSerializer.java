package roaringbitmap.utils;

import org.apache.hadoop.io.BytesWritable;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.*;
import java.nio.ByteBuffer;
public class RoaringBitmapSerializer {

    public static BytesWritable serialize(Roaring64NavigableMap bitmap) {
        try {
            if (bitmap.serializedSizeInBytes() > Integer.MAX_VALUE - 8) {
                bitmap.runOptimize();
            }
            if (bitmap.serializedSizeInBytes() > Integer.MAX_VALUE - 8) {
                throw new RuntimeException("Beyond the max number of array elements");
            }
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            bitmap.serialize(dos);
            dos.close();
            return new BytesWritable(bos.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Roaring64NavigableMap deserialize(BytesWritable bytes) {
        try {
            Roaring64NavigableMap bitmap = new Roaring64NavigableMap();
            DataInputStream dis=new DataInputStream(new ByteArrayInputStream(bytes.getBytes()));
            bitmap.deserialize(dis);
            dis.close();
            return bitmap;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}