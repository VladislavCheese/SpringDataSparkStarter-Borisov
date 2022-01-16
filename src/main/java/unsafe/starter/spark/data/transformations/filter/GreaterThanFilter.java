package unsafe.starter.spark.data.transformations.filter;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.springframework.stereotype.Component;
import unsafe.starter.spark.data.OrderedBag;

import java.util.List;

@Component("greaterThan")
public class GreaterThanFilter implements FilterSparkTransformation {
    @Override
    public Dataset<Row> transform(Dataset<Row> dataset, List<String> fieldNames, OrderedBag<Object> methodArgs) {
        return dataset.filter(functions.col(fieldNames.get(0)).geq(methodArgs.takeAndRemove()));
    }
}
