package com.windlike.io.util;

/**
 * Created by windlike.xu on 2017/12/20.
 */
public class MinHeap<T extends Comparable<? super T>>{
    public T[] getData() {
        return data;
    }

    // 堆的存储结构 - 数组
    private T[] data;

    private int size;

    // 将一个数组传入构造方法，并转换成一个小根堆
    public MinHeap(T[] data)
    {
        this.data = data;
        size = data.length;
        buildHeap();
    }

    // 将数组转换成最小堆
    private void buildHeap()
    {
        // 完全二叉树只有数组下标小于或等于 (data.length) / 2 - 1 的元素有孩子结点，遍历这些结点。
        // *比如上面的图中，数组有10个元素， (data.length) / 2 - 1的值为4，a[4]有孩子结点，但a[5]没有*
        for (int i = (size) / 2 - 1; i >= 0; i--)
        {
            // 对有孩子结点的元素heapify
            heapify(i);
        }
    }

    public void heapify(int i)
    {
        // 获取左右结点的数组下标
        int l = left(i);
        int r = right(i);

        // 这是一个临时变量，表示 跟结点、左结点、右结点中最小的值的结点的下标
        int smallest = i;

        // 存在左结点，且左结点的值小于根结点的值
        if (l < size && data[l].compareTo(data[i]) < 0)
            smallest = l;

        // 存在右结点，且右结点的值小于以上比较的较小值
        if (r < size && data[r].compareTo(data[smallest]) < 0)
            smallest = r;

        // 左右结点的值都大于根节点，直接return，不做任何操作
        if (i == smallest)
            return;

        // 交换根节点和左右结点中最小的那个值，把根节点的值替换下去
        swap(i, smallest);

        // 由于替换后左右子树会被影响，所以要对受影响的子树再进行heapify
        heapify(smallest);
    }

    // 获取右结点的数组下标
    private int right(int i)
    {
        return (i + 1) << 1;
    }

    // 获取左结点的数组下标
    private int left(int i)
    {
        return ((i + 1) << 1) - 1;
    }

    // 交换元素位置
    private void swap(int i, int j)
    {
        T tmp = data[i];
        data[i] = data[j];
        data[j] = tmp;
    }

    // 获取对中的最小的元素，根元素
    public T getRoot()
    {
        return data[0];
    }

    // 替换根元素，并重新heapify
    public void setRoot(T root)
    {
        data[0] = root;
        heapify(0);
    }

    public T removeMin(){
        if(size <= 0){
            return null;
        }
        T min = data[0];
        swap(0,  size - 1);//和尾部调换位置
        size--;//移除尾部节点
        heapify(0);//堆重建
        return min;
    }




    /*************************** test **********************************/
    public static void main(String[] args)
    {
        // 源数据
        Integer[] data = {56,275,12,6,45,478,41,1236,456,13,546,45, 500, 600,31, 1,232,44,57,76};

        topK(data, 15);

//        for(int i=9;i>=0;i--)
//        {
//            System.out.println(top5[i]);
//        }
    }

    // 从data数组中获取最大的k个数
    private static Integer[] topK(Integer[] data,int k)
    {
        // 先取K个元素放入一个数组topk中
        Integer[] topk = new Integer[k];
        for(int i = 0;i< k;i++)
        {
            topk[i] = data[i];
        }

        // 转换成最小堆
        MinHeap<Integer> heap = new MinHeap<>(topk);

        // 从k开始，遍历data
        for(int i= k;i<data.length;i++)
        {
            int root = heap.getRoot();

            // 当数据大于堆中最小的数（根节点）时，替换堆中的根节点，再转换成堆
            if(data[i] > root)
            {
                heap.setRoot(data[i]);
            }
        }

        Integer min;
        while((min = heap.removeMin()) != null)
        {
            System.out.println(min);
        }

        return topk;
    }
}
