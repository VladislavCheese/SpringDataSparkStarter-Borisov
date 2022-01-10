package unsafe.starter.spark.data.extractors.impl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;
import unsafe.starter.spark.data.extractors.DataExtractor;

@Component("csv")
public class CsvDataExtractor implements DataExtractor {
    @Override
    public Dataset<Row> readData(String pathToData, ConfigurableApplicationContext context) {
        return context.getBean(SparkSession.class).read().option("header",true).option("inferSchema",true).csv(pathToData);
    }
}
