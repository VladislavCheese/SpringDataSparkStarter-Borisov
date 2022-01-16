package unsafe.starter.spark.data.finalizers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import unsafe.starter.spark.data.OrderedBag;

public interface Finalizer {
    Object doFinalAction(Dataset<Row> dataset, Class<?> modelClass, OrderedBag<Object> args);
}
