package com.edui.location.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.util.StringUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author edui
 * @date 2022/7/5
 */
public class IndexFileUtil {

    private IndexHeader indexHeader;

    private byte[] index;

    private byte[] data;

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

    /**
     * 将数据写入文件进行持久化
     *
     * @param path 持久化的路劲
     */
    private void writeToFile(String path) throws IOException {
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

    /**
     * 构建IP搜索文件
     */
    public void createIpv4IndexFile() throws Exception {
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
        String path = "D:\\IP\\ip.txt";
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
        writeToFile("D:\\IP\\ipv4data.db");
    }

    /**
     * 构建号码归属地搜索文件
     */
    public void createPhoneIndexFile() throws Exception {
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
        String path = "D:\\mobile\\mobile.json";
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
        writeToFile("D:\\mobile\\phonedata.db");
    }

    public static void main(String[] args) throws Exception {
        IndexFileUtil indexFileUtil = new IndexFileUtil();
        indexFileUtil.createPhoneIndexFile();
        indexFileUtil.createIpv4IndexFile();
    }

}
