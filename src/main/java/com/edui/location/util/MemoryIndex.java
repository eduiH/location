package com.edui.location.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.util.StringUtils;

import java.io.*;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author edui
 * @Date 2022/7/4
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
     * 将数据写入文件进行持久化
     *
     * @param path 持久化的路劲
     */
    public void writeToFile(String path) throws IOException {
        //将文件头、索引、数据依次放到文件中
        try (FileOutputStream outputStream = new FileOutputStream(path, true)) {
            outputStream.write(this.indexHeader.toHeaderByte());
            byte[] indexLengthByte = new byte[indexHeader.getIndexPrimaryKeyByteLength()];
            int indexBlockLength = this.indexHeader.getIndexPrimaryKeyByteLength() + this.indexHeader.getDataNumByteLength();
            int indexLength = this.index.length / indexBlockLength;
            for (int i = indexHeader.getIndexPrimaryKeyByteLength() - 1; i >= 0; i--) {
                indexLengthByte[indexHeader.getIndexPrimaryKeyByteLength() - 1 - i] = (byte) ((indexLength & (0xFF << i * 8)) >> i * 8);
            }
            outputStream.write(indexLengthByte);

            outputStream.write(this.index);
            outputStream.write(this.data);
        }
    }

    public String findPhoneAddr(String phone){
        String prefix = phone.substring(0,7);
        int prefixInt = Integer.valueOf(prefix);
        byte[] prefixByte  =new byte[indexHeader.getIndexPrimaryKeyByteLength()];
        for (int i = indexHeader.getIndexPrimaryKeyByteLength() - 1; i >= 0; i--) {
            prefixByte[indexHeader.getIndexPrimaryKeyByteLength() - 1 - i] = (byte) ((prefixInt & (0xFF << i * 8)) >> i * 8);
        }
        return this.find(prefixByte);
    }

    public String findIpAddr(String ip) throws Exception {
        InetAddress inetAddress = InetAddress.getByName(ip);
        if (this.indexHeader.getIndexPrimaryKeyByteLength() != 4) {
            throw new Exception("not supported range");
        }
        return this.find(inetAddress.getAddress());
    }

    /**
     * 通过二分法搜索返回数据
     *
     * @param primaryKey 查询的数据库
     * @return 返回数据
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
     * @param targetVal 目标值偏移量
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
     * 长整型转成IP的byte的形式
     *
     * @param target 目标整形
     * @return 返回byte数组
     */
    private byte[] longToIpByte(long target) {
        byte[] result = new byte[4];
        for (int i = indexHeader.getIndexPrimaryKeyByteLength() - 1; i >= 0; i--) {
            result[indexHeader.getIndexPrimaryKeyByteLength() - 1 - i] = (byte) ((target & (0xFF << i * 8)) >> i * 8);
        }
        return result;
    }

    MemoryIndex() throws Exception {
        //createIpv4IndexFile();
        createPhoneIndexFile();
    }

    /**
     * 构建IP搜索文件
     */
    private void createIpv4IndexFile() throws Exception {
        //头文件参数设置
        this.indexHeader = new IndexHeader(
                1,
                6,
                4,
                3,
                1
        );
        //一个索引的长度
        int indexBlockLength = this.indexHeader.getIndexPrimaryKeyByteLength() + this.indexHeader.getDataNumByteLength();
        //数据去重用的
        Map<String, Integer> dataOffsetMap = new HashMap<>();
        //保存前一个索引对应的数据，用于合并相同数据的索引
        String preProvince = null;
        //按行读取文件
        String path = "D:\\ip.txt";
        try (FileInputStream inputStream = new FileInputStream(path)) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String line = null;
            while ((line = reader.readLine()) != null) {

                //根据源数据文件规则分割处理
                String[] str = line.split("\\$");

                byte[] indexBlock = new byte[indexBlockLength];

                //分离出IP， IPv4固定四位
                byte[] keyByte = this.longToIpByte(Long.valueOf(str[0]));
                indexBlock[0] = keyByte[0];
                indexBlock[1] = keyByte[1];
                indexBlock[2] = keyByte[2];
                indexBlock[3] = keyByte[3];


                //分离出数据
                int j = 2;
                while (j < str.length - 1 && !StringUtils.hasText(str[j])) {
                    j++;
                }
                String province = str[j];


                //数据相同的直接合并到前一个索引即可
                if (province.equals(preProvince)) {
                    continue;
                } else {
                    preProvince = province;
                }

                //数据去重，如果原来已经有的数据直接使用
                Integer currentDataOffset = dataOffsetMap.get(province);
                int dataOffset;
                if (currentDataOffset == null) {
                    if (this.data == null) {
                        dataOffset = 0;
                    } else {
                        dataOffset = this.data.length;
                    }
                    byte[] newDataTemp = province.getBytes(this.indexHeader.charsetName());

                    //数据长度信息
                    byte[] dataLength = new byte[this.indexHeader.getDataLengthLimitByteLength()];
                    for (int i = 0; i < this.indexHeader.getDataLengthLimitByteLength(); i++) {
                        dataLength[i] = (byte) (newDataTemp.length >> (this.indexHeader.getDataLengthLimitByteLength() - i - 1) * 8);
                    }

                    byte[] newData = new byte[newDataTemp.length + dataOffset + this.indexHeader.getDataLengthLimitByteLength()];
                    if (this.data != null) {
                        System.arraycopy(this.data, 0, newData, 0, dataOffset);
                    }

                    //增加数据长度信息在数据block头部
                    System.arraycopy(dataLength, 0, newData, dataOffset, dataLength.length);

                    //增加数据
                    System.arraycopy(newDataTemp, 0, newData, dataOffset + this.indexHeader.getDataLengthLimitByteLength(), newDataTemp.length);

                    this.data = newData;
                } else {
                    dataOffset = currentDataOffset;
                }

                //记录索引对应的数据位置
                for (int i = 0; i < this.indexHeader.getDataNumByteLength(); i++) {
                    indexBlock[4 + i] = (byte) (dataOffset >> (this.indexHeader.getDataNumByteLength() - i - 1) * 8);
                }

                //增加一条索引数据
                int indexOffset;
                if (this.index == null) {
                    indexOffset = 0;
                } else {
                    indexOffset = this.index.length;
                }
                byte[] newIndex = new byte[indexBlock.length + indexOffset];
                if (this.index != null) {
                    System.arraycopy(this.index, 0, newIndex, 0, indexOffset);
                }
                System.arraycopy(indexBlock, 0, newIndex, indexOffset, indexBlock.length);
                this.index = newIndex;
            }
        }
        writeToFile("D:\\IP\\data.db");
    }

    /**
     * 构建号码归属地搜索文件
     */
    private void createPhoneIndexFile() throws Exception {
        //头文件参数设置
        this.indexHeader = new IndexHeader(
                1,
                6,
                3,
                3,
                1
        );
        //一个索引的长度
        int indexBlockLength = this.indexHeader.getIndexPrimaryKeyByteLength() + this.indexHeader.getDataNumByteLength();
        //数据去重用的
        Map<String, Integer> dataOffsetMap = new HashMap<>();
        //保存前一个索引对应的数据，用于合并相同数据的索引
        String preProvince = null;
        //按行读取文件
        String path = "D:\\mobile.json";
        try (FileInputStream inputStream = new FileInputStream(path)) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String line = null;
            while ((line = reader.readLine()) != null) {

                ObjectMapper mapper = new ObjectMapper();
                ArrayList<HashMap<String, String>> allList = mapper.readValue(line, ArrayList.class);
                for (HashMap<String, String> stringStringHashMap : allList) {

                    byte[] indexBlock = new byte[indexBlockLength];

                    //获取号段， 号段固定三位
                    byte[] keyByte = this.longToIpByte(Long.valueOf(stringStringHashMap.get("Prefix")));
                    indexBlock[0] = keyByte[0];
                    indexBlock[1] = keyByte[1];
                    indexBlock[2] = keyByte[2];


                    //获取归属地数据
                    String province = stringStringHashMap.get("Province")+stringStringHashMap.get("City");

                    //数据相同的直接合并到前一个索引即可
                    if (province.equals(preProvince)) {
                        continue;
                    } else {
                        preProvince = province;
                    }

                    //数据去重，如果原来已经有的数据直接使用
                    Integer currentDataOffset = dataOffsetMap.get(province);
                    int dataOffset;
                    if (currentDataOffset == null) {
                        if (this.data == null) {
                            dataOffset = 0;
                        } else {
                            dataOffset = this.data.length;
                        }
                        byte[] newDataTemp = province.getBytes(this.indexHeader.charsetName());

                        //数据长度信息
                        byte[] dataLength = new byte[this.indexHeader.getDataLengthLimitByteLength()];
                        for (int i = 0; i < this.indexHeader.getDataLengthLimitByteLength(); i++) {
                            dataLength[i] = (byte) (newDataTemp.length >> (this.indexHeader.getDataLengthLimitByteLength() - i - 1) * 8);
                        }

                        byte[] newData = new byte[newDataTemp.length + dataOffset + this.indexHeader.getDataLengthLimitByteLength()];
                        if (this.data != null) {
                            System.arraycopy(this.data, 0, newData, 0, dataOffset);
                        }

                        //增加数据长度信息在数据block头部
                        System.arraycopy(dataLength, 0, newData, dataOffset, dataLength.length);

                        //增加数据
                        System.arraycopy(newDataTemp, 0, newData, dataOffset + this.indexHeader.getDataLengthLimitByteLength(), newDataTemp.length);

                        this.data = newData;
                    } else {
                        dataOffset = currentDataOffset;
                    }

                    //记录索引对应的数据位置
                    for (int i = 0; i < this.indexHeader.getDataNumByteLength(); i++) {
                        indexBlock[3 + i] = (byte) (dataOffset >> (this.indexHeader.getDataNumByteLength() - i - 1) * 8);
                    }

                    //增加一条索引数据
                    int indexOffset;
                    if (this.index == null) {
                        indexOffset = 0;
                    } else {
                        indexOffset = this.index.length;
                    }
                    byte[] newIndex = new byte[indexBlock.length + indexOffset];
                    if (this.index != null) {
                        System.arraycopy(this.index, 0, newIndex, 0, indexOffset);
                    }
                    System.arraycopy(indexBlock, 0, newIndex, indexOffset, indexBlock.length);
                    this.index = newIndex;
                }
            }
        }
        writeToFile("D:\\mobile\\data.db");
    }

    public static void main(String[] args) throws Exception {
        MemoryIndex memoryIndex = new MemoryIndex("D:\\data.db");
        System.out.println(memoryIndex.findPhoneAddr("13125159898"));
    }

}
