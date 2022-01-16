package unsafe.starter.spark.data.lazy.collections;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ConfigurableApplicationContext;
import unsafe.starter.spark.data.extractors.DataExtractor;
import unsafe.starter.spark.data.extractors.DataExtractorResolver;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FirstLevelCacheService {

    @Autowired
    private DataExtractorResolver resolver;

    private Map<Class<?>, Dataset<Row>> model2Dataset = new HashMap<>();

    public List getDataFor(long ownerId, String foreignKeyName, Class<?> model, String path, ConfigurableApplicationContext context) {
        if (!model2Dataset.containsKey(model)) {
            DataExtractor dataExtractor = resolver.resolve(path);
            Dataset<Row> dataset = dataExtractor.readData(path, context).persist();
            model2Dataset.put(model, dataset);
        }
        return model2Dataset.get(model)
                .filter(functions.col(foreignKeyName).equalTo(ownerId))
                .as(Encoders.bean(model))
                .collectAsList();
    }
}
