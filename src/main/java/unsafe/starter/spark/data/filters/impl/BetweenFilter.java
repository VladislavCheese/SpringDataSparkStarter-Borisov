package unsafe.starter.spark.data.filters.impl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.springframework.stereotype.Component;
import unsafe.starter.spark.data.filters.FilterSparkTransformation;

import java.util.List;

@Component("between")
public class BetweenFilter implements FilterSparkTransformation {
    @Override
    public Dataset<Row> transform(Dataset<Row> dataset, List<String> argNames, OrderedBag<Object> args) {
       return dataset.filter(functions.col(argNames.get(0)).between(args.takeAndRemove(),args.takeAndRemove()));
    }
}
