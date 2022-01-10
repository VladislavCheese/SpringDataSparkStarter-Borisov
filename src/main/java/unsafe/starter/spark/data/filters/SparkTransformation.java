package unsafe.starter.spark.data.filters;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import unsafe.starter.spark.data.filters.impl.OrderedBag;

import java.util.List;

public interface SparkTransformation {
    /**
     * Преобразование коллекции объектов.
     * @param dataset данные spark
     * @param fieldNames передаем списком тк может понадобиться несколько в некоторых фильтрах
     * @param methodArgs аргументы метода
     * @return обработанные данные spark
     */
    Dataset<Row> transform(Dataset<Row> dataset, List<String> fieldNames, OrderedBag<Object> methodArgs);
}
