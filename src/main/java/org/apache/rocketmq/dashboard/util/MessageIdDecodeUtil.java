package org.apache.rocketmq.dashboard.util;

import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.SecureRandom;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.Random;
import java.util.TimeZone;
import org.apache.rocketmq.client.java.message.MessageIdCodec;
import org.apache.rocketmq.client.java.misc.Utilities;
import org.apache.rocketmq.common.MQVersion.Version;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageClientIDSetter;

/**
 * @author houjuntao
 * @date 2024-03-07 14:46
 */
public class MessageIdDecodeUtil {
    private static final Random RANDOM = new SecureRandom();

    public static String getAddressStrFromID(String msgId, String producerMqVersion) {
        String address;

        if (producerMqVersion.compareTo(Version.V5_0_0.name()) < 0) {
            address = MessageClientIDSetter.getIPStrFromID(msgId);
        } else {
            if (MessageIdCodec.MESSAGE_ID_LENGTH_FOR_V1_OR_LATER == msgId.length()) {
                msgId = msgId.substring(2);
            }

            // 16进制字符串转10进制字节数据中
            byte[] bytes = UtilAll.string2bytes(msgId);
            ByteBuffer buf = ByteBuffer.allocate(6);
            buf.order(ByteOrder.BIG_ENDIAN);
            buf.put(bytes, 0, 6);
            buf.position(0);
            byte[] macAddress = new byte[6];
            System.arraycopy(bytes, 0, macAddress, 0, 6);
            address = Utilities.encodeHexString(macAddress, false);
        }

        return address;
    }

    public static byte[] getAddressFromID(String msgId, String producerMqVersion) {
        byte[] address;

        if (producerMqVersion.compareTo(Version.V5_0_0.name()) < 0) {
            address = MessageClientIDSetter.getIPFromID(msgId);
        } else {
            if (MessageIdCodec.MESSAGE_ID_LENGTH_FOR_V1_OR_LATER == msgId.length()) {
                msgId = msgId.substring(2);
            }

            // 16进制字符串转10进制字节数据中
            byte[] bytes = UtilAll.string2bytes(msgId);
            ByteBuffer buf = ByteBuffer.allocate(6);
            buf.order(ByteOrder.BIG_ENDIAN);
            buf.put(bytes, 0, 6);
            buf.position(0);
            address = new byte[6];
            System.arraycopy(bytes, 0, address, 0, 6);
        }

        return address;
    }

    public static int getPidFromID(String msgId, String producerMqVersion) {
        int pid;

        if (producerMqVersion.compareTo(Version.V5_0_0.name()) < 0) {
            pid = MessageClientIDSetter.getPidFromID(msgId);
        } else {
            if (MessageIdCodec.MESSAGE_ID_LENGTH_FOR_V1_OR_LATER == msgId.length()) {
                msgId = msgId.substring(2);
            }

            // 16进制字符串转10进制字节数据中
            byte[] bytes = UtilAll.string2bytes(msgId);
            // process id(lower 2bytes)
            ByteBuffer buf = ByteBuffer.allocate(4);
            buf.order(ByteOrder.BIG_ENDIAN);
            buf.put((byte) 0);
            buf.put((byte) 0);
            buf.put(bytes, 6, 2);
            buf.position(0);
            pid = buf.getInt();
        }

        return pid;
    }

    /**
     * @param msgId             生产者发生消息时生成的消息ID
     * @param producerMqVersion 生产者RocketMQ版本号
     * @return
     */
    public static Date getNearlyTimeFromID(String msgId, String producerMqVersion) {
        Date messageSendDate;

        if (producerMqVersion.compareTo(Version.V5_0_0.name()) < 0) {
            messageSendDate = MessageClientIDSetter.getNearlyTimeFromID(msgId);
        } else {
            if (MessageIdCodec.MESSAGE_ID_LENGTH_FOR_V1_OR_LATER == msgId.length()) {
                msgId = msgId.substring(2);
            }

            // 16进制字符串转10进制字节数据中
            byte[] bytes = UtilAll.string2bytes(msgId);
            ByteBuffer buf = ByteBuffer.allocate(4);
            buf.order(ByteOrder.BIG_ENDIAN);
            buf.put(bytes, 8, 4);
            buf.position(0);
            int seconds = buf.getInt();

            Calendar calendar = MessageIdDecodeUtil.customEpochMillis();
            calendar.add(Calendar.SECOND, seconds);
            messageSendDate = calendar.getTime();
        }

        return messageSendDate;
    }

    private static Calendar customEpochMillis() {
        // 2021-01-01 00:00:00
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("Etc/UTC"));
        calendar.set(Calendar.YEAR, 2021);
        calendar.set(Calendar.MONTH, Calendar.JANUARY);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        return calendar;
    }

    public static void macAddress() {
        try {
            final Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                final NetworkInterface networkInterface = networkInterfaces.nextElement();
                final byte[] mac = networkInterface.getHardwareAddress();

                if (null == mac) {
                    continue;
                }

                System.out.println("mac address is " + Utilities.encodeHexString(mac, false));
            }
        } catch (Throwable ignore) {
            // Ignore on purpose.
        }

        byte[] randomBytes = new byte[6];
        MessageIdDecodeUtil.RANDOM.nextBytes(randomBytes);
        System.out.println("random mac address is " + Utilities.encodeHexString(randomBytes, false));
    }

    public static void main(String[] args) {
        MessageIdDecodeUtil.macAddress();
        System.out.println();

        MessageIdCodec.getInstance().nextMessageId();

        // 63625
        String messageV5X = "01FA03C86D77D4000005FB145600000000";
        System.out.println("macAddress is " + MessageIdDecodeUtil.getAddressStrFromID(messageV5X, Version.V5_1_0.name()));
        System.out.println("macAddress is " + Utilities.encodeHexString(MessageIdDecodeUtil.getAddressFromID(messageV5X, Version.V5_1_0.name()), false));
        System.out.println("pid is " + MessageIdDecodeUtil.getPidFromID(messageV5X, Version.V5_1_0.name()));
        System.out.println(
                UtilAll.formatDate(MessageIdDecodeUtil.getNearlyTimeFromID(messageV5X, Version.V5_1_0.name()),
                        UtilAll.YYYY_MM_DD_HH_MM_SS));

        String messageV4X = MessageClientIDSetter.createUniqID();
        System.out.println(
                UtilAll.formatDate(MessageIdDecodeUtil.getNearlyTimeFromID(messageV4X, Version.V4_9_7.name()),
                        UtilAll.YYYY_MM_DD_HH_MM_SS));
    }
}
