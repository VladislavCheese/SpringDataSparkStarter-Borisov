package unsafe.starter.spark.data.spiders;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import scala.Tuple2;
import unsafe.starter.spark.data.WordsMatcher;
import unsafe.starter.spark.data.transformations.filter.FilterSparkTransformation;
import unsafe.starter.spark.data.transformations.SparkTransformation;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Component("findBy")
@RequiredArgsConstructor
public class FilterTransformationSpider implements TransformationSpider {

    private final Map<String, FilterSparkTransformation> filterTransformation;

    @Override
    public Tuple2<SparkTransformation,List<String>> createTransformation(List<String> remainingWords, Set<String> fieldNames) {
        String fieldName = WordsMatcher.findAndRemoveMatchingPiecesIfExists(fieldNames, remainingWords);
        String filterName = WordsMatcher.findAndRemoveMatchingPiecesIfExists(filterTransformation.keySet(), remainingWords);
        return new Tuple2<>(filterTransformation.get(filterName), List.of(fieldName));
    }
}
