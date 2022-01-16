package unsafe.starter.spark.data.transformations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;
import unsafe.starter.spark.data.OrderedBag;

import java.util.List;

@Component
public class SortTransformation implements SparkTransformation {
    @Override
    public Dataset<Row> transform(Dataset<Row> dataset, List<String> fieldNames, OrderedBag<Object> methodArgs) {
        return dataset.orderBy(fieldNames.get(0), fieldNames.stream().skip(1).toArray(String[]::new));
    }
}
