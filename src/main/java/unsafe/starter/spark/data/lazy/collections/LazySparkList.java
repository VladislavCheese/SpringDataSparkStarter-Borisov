package unsafe.starter.spark.data.lazy.collections;

import lombok.Data;
import lombok.experimental.Delegate;

import java.util.List;

@Data
public class LazySparkList implements List {

    @Delegate
    private List content;

    private long ownerId;
    private String foreignKeyName;
    private Class<?> modelClass;
    private String pathToSource;


    public boolean initialized(){
        return (content != null && !content.isEmpty());
    }
}
