package com.edui.location.util;

import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;

/**
 * @author edui
 * @date 2022/7/4
 */
public class MemoryIndex {

    private IndexHeader indexHeader;

    private byte[] index;

    private byte[] data;

    public MemoryIndex(String path) throws Exception {
        //读取文件头
        indexHeader = new IndexHeader(path);
        //因为Java数组最大是int类型四个字节太长的装不下
        if (indexHeader.getIndexPrimaryKeyByteLength() > 4) {
            throw new Exception("not supported range");
        }
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(path, "r")) {
            //根据文件头的主键长度配置，获取索引数据块的数量，索引主键多长这个数据就多长
            byte[] indexBlockNumByte = new byte[indexHeader.getIndexPrimaryKeyByteLength()];
            randomAccessFile.seek(IndexHeader.HEADER_DATA_LENGTH);
            randomAccessFile.read(indexBlockNumByte, 0, indexBlockNumByte.length);
            int indexBlockNum = 0;
            for (int i = 0; i < indexBlockNumByte.length; i++) {
                indexBlockNum = (indexBlockNum << 8) + (indexBlockNumByte[i] & 0xFF);
            }

            //读取索引信息
            int indexBlockLength = this.indexHeader.getIndexPrimaryKeyByteLength() + this.indexHeader.getDataNumByteLength();
            this.index = new byte[indexBlockNum * indexBlockLength];
            int indexStartOffset = IndexHeader.HEADER_DATA_LENGTH + indexHeader.getIndexPrimaryKeyByteLength();
            randomAccessFile.seek(indexStartOffset);
            randomAccessFile.read(this.index, 0, this.index.length);

            //读取数据信息
            int dataStartOffset = indexStartOffset + this.index.length;
            int dataLength = (int) (randomAccessFile.length() - dataStartOffset);
            this.data = new byte[dataLength];
            randomAccessFile.seek(dataStartOffset);
            randomAccessFile.read(this.data, 0, dataLength);
        }
    }

    /**
     * 通过二分法搜索返回数据
     *
     * @param primaryKey 查询的数据主键
     */
    private String find(byte[] primaryKey) {
        //一个索引占用的byte数量和所有索引的数量
        int indexBlockLength = this.indexHeader.getIndexPrimaryKeyByteLength() + this.indexHeader.getDataNumByteLength();
        int indexBlockNum = this.index.length / indexBlockLength;

        //二分法搜索到对应的索引数组下表
        int targetIndexBlockIndex = 0;
        int left = 0, right = indexBlockNum - 1;
        while ((right - left) > 1) {
            int min = (left + right) / 2;
            byte[] target = new byte[this.indexHeader.getIndexPrimaryKeyByteLength()];
            System.arraycopy(this.index, min * indexBlockLength, target, 0, target.length);
            if (compare(primaryKey, target) == 0) {
                targetIndexBlockIndex = min;
                break;
            } else if (compare(primaryKey, target) > 0) {
                left = min;
            } else {
                right = min;
            }
        }
        //边界条件，如果目标所以没有则返回前一个的值，支持范围搜索
        //比如[1,3,5,9]，搜索4，则返回的索引主键为3对应的数据
        if (targetIndexBlockIndex == 0) {
            byte[] target = new byte[this.indexHeader.getIndexPrimaryKeyByteLength()];
            System.arraycopy(this.index, right * indexBlockLength, target, 0, target.length);
            if (compare(primaryKey, target) == 0) {
                targetIndexBlockIndex = right;
            } else {
                targetIndexBlockIndex = left;
            }
        }

        //取出目标索引
        byte[] targetIndex = new byte[indexBlockLength];
        System.arraycopy(this.index, targetIndexBlockIndex * indexBlockLength, targetIndex, 0, targetIndex.length);

        //获取数据的位置
        int dataOffset = 0;
        for (int i = 0; i < this.indexHeader.getDataNumByteLength(); i++) {
            dataOffset = (dataOffset << 8) + (targetIndex[targetIndex.length - indexHeader.getDataNumByteLength() + i] & 0xFF);
        }

        //获取目标数据的长度
        byte[] targetDataLength = new byte[this.indexHeader.getDataLengthLimitByteLength()];
        System.arraycopy(this.data, dataOffset, targetDataLength, 0, targetDataLength.length);
        int dataLength = 0;
        for (int i = 0; i < this.indexHeader.getDataLengthLimitByteLength(); i++) {
            dataLength = (dataLength << 8) + (targetDataLength[targetDataLength.length - this.indexHeader.getDataLengthLimitByteLength() + i] & 0xFF);
        }

        //取出数据，根据文件头的配置进行编码返回
        byte[] dataResult = new byte[dataLength];
        System.arraycopy(this.data, dataOffset + this.indexHeader.getDataLengthLimitByteLength(), dataResult, 0, dataLength);
        try {
            return new String(dataResult, this.indexHeader.charsetName());
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("error index header code = " + this.indexHeader.getCode());
        }
    }

    /**
     * 比较
     *
     * @param targetVal 目标值
     * @param val       比较值
     * @return 相等返回0，大于目标返回>0, 小于目标返回<0
     */
    private int compare(byte[] val, byte[] targetVal) {
        for (int i = 0; i < targetVal.length; i++) {
            if ((val[i] & 0xff) > (targetVal[i] & 0xff)) {
                return 1;
            } else if ((val[i] & 0xff) < (targetVal[i] & 0xff)) {
                return -1;
            }
        }
        return 0;
    }

    /**
     * 号码归属地查询
     * @param phone 目标号码
     */
    public String findPhoneAddr(String phone) throws Exception{
        //号码的前七位为号段
        if(phone == null || phone.length() < 7){
            throw new Exception("not supported phone");
        }
        String prefix = phone.substring(0,7);
        int prefixInt = Integer.valueOf(prefix);
        byte[] prefixByte  =new byte[indexHeader.getIndexPrimaryKeyByteLength()];
        for (int i = indexHeader.getIndexPrimaryKeyByteLength() - 1; i >= 0; i--) {
            prefixByte[indexHeader.getIndexPrimaryKeyByteLength() - 1 - i] = (byte) ((prefixInt & (0xFF << i * 8)) >> i * 8);
        }
        return this.find(prefixByte);
    }

    /**
     * IP归属地查询
     * @param ip 目标IP
     */
    public String findIpAddr(String ip) throws Exception {
        InetAddress inetAddress = InetAddress.getByName(ip);
        if (this.indexHeader.getIndexPrimaryKeyByteLength() != 4) {
            throw new Exception("not supported range");
        }
        return this.find(inetAddress.getAddress());
    }

}
