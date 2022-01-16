package unsafe.starter.spark.data;

import java.util.LinkedList;
import java.util.List;

import static java.util.Arrays.asList;

public class OrderedBag<T> {
    List<T> list;

    public OrderedBag(T[] args) {
        this.list = new LinkedList<>(asList(args));
    }

    public T takeAndRemove(){
        return list.remove(0);
    }
    public int size(){
        return list.size();
    }
}
