package unsafe.starter.spark.data.api;

import scala.Tuple2;
import unsafe.starter.spark.data.filters.SparkTransformation;

import java.util.List;
import java.util.Set;

public interface TransformationSpider {
    Tuple2<SparkTransformation,List<String>> createTransformation(List<String> remainingWords, Set<String> fieldNames);
}
