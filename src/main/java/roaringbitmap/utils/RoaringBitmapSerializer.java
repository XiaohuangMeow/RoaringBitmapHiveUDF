package roaringbitmap.utils;

import org.apache.hadoop.io.BytesWritable;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;
import java.nio.ByteBuffer;

public class RoaringBitmapSerializer {

    public static BytesWritable serialize(Roaring64Bitmap bitmap) {
        try {
            if (bitmap.serializedSizeInBytes() > Integer.MAX_VALUE - 8) {
                bitmap.runOptimize();
            }
            if (bitmap.serializedSizeInBytes() > Integer.MAX_VALUE - 8) {
                throw new RuntimeException("Beyond the max number of array elements");
            }
            byte[] bytes = new byte[(int) bitmap.serializedSizeInBytes()];
            bitmap.serialize(ByteBuffer.wrap(bytes));
            return new BytesWritable(bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Roaring64Bitmap deserialize(BytesWritable bytes) {
        try {
            Roaring64Bitmap bitmap = new Roaring64Bitmap();
            bitmap.deserialize(ByteBuffer.wrap(bytes.getBytes()));
            return bitmap;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
