package com.mogujie.jessica.store;

/**
 * java位运算简单说明<br/>
 * java中有4中位运算，分别是按位与&，按位或|，按位异或^，按位取反。<br/>
 * &：两位全为1，结果为1<br/>
 * |：两位有一个为1，结果为1<br/>
 * ^：两位有一个为1，一个为0，结果为1<br/>
 * ~：0取反位1,1取反位0<br/>
 * 
 * java中有3个移位运算符<br/>
 * >> 算术右移：低位溢出，符号位不变，并用符号位补溢出的高位<br/>
 * <<算术左移：符号位不变，低位补0<br/>
 * >>>逻辑右移：低位溢出，高位补0<br/>
 * 
 * @author xuanxi
 * 
 */
public class Pointer
{
    public int poolIdx;
    public int sliceIdx;
    public int offsetIdx;
    public int pointer;

    public Pointer(int pointer)
    {
        this.pointer = pointer;
        this.poolIdx = pointer >>> 30;
        this.sliceIdx = (pointer & 0x3FFFFFFF) >>> PostingListStore.INT_SLICE_SIZE_SHIFT[poolIdx];
        this.offsetIdx = pointer & PostingListStore.INT_SLICE_SIZE_MASK[poolIdx];

    }

    public Pointer(int poolIdx, int sliceIdx, int offsetIdx)
    {
        this.poolIdx = poolIdx;
        this.sliceIdx = sliceIdx;
        this.offsetIdx = offsetIdx;
        this.pointer = ((this.poolIdx << 30) | (this.sliceIdx << PostingListStore.INT_SLICE_SIZE_SHIFT[poolIdx])) | this.offsetIdx;

    }
    
    public static void main(String[] args)
    {
        System.err.println(new Pointer(1073741826).poolIdx);
    }
    
}