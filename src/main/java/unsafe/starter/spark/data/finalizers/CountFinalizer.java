package unsafe.starter.spark.data.finalizers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;
import unsafe.starter.spark.data.OrderedBag;

@Component("count")
public class CountFinalizer implements Finalizer {
    @Override
    public Object doFinalAction(Dataset<Row> dataset, Class<?> modelClass, OrderedBag<Object> args) {
        return dataset.count();
    }
}
