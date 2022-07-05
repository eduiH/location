package com.edui.location.util;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

/**
 * 文件头保存的数据
 *
 * @author edui
 * @date 2022/7/4
 */
public class IndexHeader {

    /**
     * 文件头长度
     */
    public final static int HEADER_DATA_LENGTH = 2;

    /**
     * 2 bit
     * 版本，保存版本信息做兼容的
     */
    private final int version;

    /**
     * 4bit
     * 数据的编码格式，保存的下面CHARSETS索引
     */
    private final int code;

    private final static String[] CHARSETS = new String[]{
            StandardCharsets.UTF_8.name(),
            StandardCharsets.US_ASCII.name(),
            StandardCharsets.ISO_8859_1.name(),
            StandardCharsets.UTF_16.name(),
            StandardCharsets.UTF_16BE.name(),
            StandardCharsets.UTF_16LE.name(),
            "GBK"
    };
    /**
     * 4bit
     * 用来保存一个索引主键数据的byte长度，影响一个索引的block的大小
     */
    private final int indexPrimaryKeyByteLength;
    /**
     * 4bit
     * 总数据长度限制byte长度，影响一个索引的block的大小
     */
    private final int dataNumByteLength;
    /**
     * 2 bit
     * 单条数据长度限制byte长度，影响数据block前缀的读取
     */
    private final int dataLengthLimitByteLength;

    public IndexHeader(int version, int code, int indexPrimaryKeyByteLength, int dataNumByteLength, int dataLengthLimitByteLength) {
        this.version = version;
        this.code = code;
        this.indexPrimaryKeyByteLength = indexPrimaryKeyByteLength;
        this.dataNumByteLength = dataNumByteLength;
        this.dataLengthLimitByteLength = dataLengthLimitByteLength;
    }

    /**
     * 从文件中读取文件头信息
     *
     * @param path 文件路劲
     */
    IndexHeader(String path) throws IOException {
        byte[] header = new byte[IndexHeader.HEADER_DATA_LENGTH];
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(path, "r")) {
            //根据格式读取请求头信息
            randomAccessFile.read(header, 0, IndexHeader.HEADER_DATA_LENGTH);
            this.version = header[0] >> 6;
            this.code = ((header[0] & 0x3C) & 0xff) >> 2;
            this.indexPrimaryKeyByteLength = (((header[0] & 0x03)& 0xff) << 2) + ((header[1]& 0xff) >> 6);
            this.dataNumByteLength = ((header[1] & 0x3C) & 0xff) >> 2;
            this.dataLengthLimitByteLength = header[1] & 0x03;
        }
    }

    /**
     * 转成byte格式用于持久化到文件中
     */
    public byte[] toHeaderByte() {
        byte[] header = new byte[2];
        header[0] = (byte) ((this.version << 6) + (this.code << 2) + (this.indexPrimaryKeyByteLength >> 2));
        header[1] = (byte) (((this.indexPrimaryKeyByteLength & 0x03) << 6) + (this.dataNumByteLength << 2) + this.dataLengthLimitByteLength);
        return header;
    }

    public String charsetName() {
        return CHARSETS[code];
    }

    public int getVersion() {
        return version;
    }

    public int getCode() {
        return code;
    }

    public static String[] getCHARSETS() {
        return CHARSETS;
    }

    public int getIndexPrimaryKeyByteLength() {
        return indexPrimaryKeyByteLength;
    }

    public int getDataNumByteLength() {
        return dataNumByteLength;
    }

    public int getDataLengthLimitByteLength() {
        return dataLengthLimitByteLength;
    }
}
