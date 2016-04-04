/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package uconn.cse.cassperf.utils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;

/**
 *
 * @author nhannguyen
 */
public class MD5hash {

    public MD5hash() {
    }

    public static byte[] hash(ByteBuffer... data) throws Exception {
        MessageDigest messageDigest = MessageDigest.getInstance("MD5");
        for (ByteBuffer block : data) {
            if (block.hasArray()) {
                messageDigest.update(block.array(), block.arrayOffset() + block.position(), block.remaining());
            } else {
                messageDigest.update(block.duplicate());
            }
        }

        return messageDigest.digest();
    }

    public long getToken(long uID) throws Exception {
        MessageDigest msgDigest = MessageDigest.getInstance("MD5");
        byte[] array = ByteBuffer.allocate(8).putLong(uID).array();
        msgDigest.update(array);

        byte byteData[] = msgDigest.digest();

        //convert the byte to hex format method 1
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < byteData.length; i++) {
            sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
        }

        System.out.println("Digest(in hex format):: " + sb.toString());

        BigInteger lHashInt = new BigInteger(1, msgDigest.digest());

        String output = lHashInt.toString(16);
        System.out.println(output);
        BigDecimal bigdec = new BigDecimal(lHashInt);
        System.out.println(bigdec.toString());
        System.out.println(String.format("%1$032X", lHashInt));

        return 1;
    }
}
