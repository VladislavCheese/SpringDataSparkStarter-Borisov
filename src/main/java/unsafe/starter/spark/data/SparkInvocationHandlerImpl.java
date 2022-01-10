package unsafe.starter.spark.data;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.context.ConfigurableApplicationContext;
import scala.Tuple2;
import unsafe.starter.spark.data.extractors.DataExtractor;
import unsafe.starter.spark.data.filters.impl.OrderedBag;
import unsafe.starter.spark.data.finalizers.Finalizer;
import unsafe.starter.spark.data.api.SparkInvocationHandler;
import unsafe.starter.spark.data.filters.SparkTransformation;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SparkInvocationHandlerImpl implements SparkInvocationHandler {
    private Class<?> modelClass;
    private String pathToData;
    private DataExtractor dataExtractor;
    private Map<Method, List<Tuple2<SparkTransformation,List<String>>>> transformationChain;
    private Map<Method, Finalizer> finalizerMap;

    private ConfigurableApplicationContext context;

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) {
        Dataset<Row> dataset = dataExtractor.readData(pathToData, context);
        OrderedBag<Object> orderedArgs = new OrderedBag<>(args);
        List<Tuple2<SparkTransformation,List<String>>> tupleList = transformationChain.get(method);
        for (Tuple2<SparkTransformation,List<String>> transformationPair : tupleList) {
            SparkTransformation transformation = transformationPair._1;
            List<String> fieldNames = transformationPair._2;
            dataset = transformation.transform(dataset, fieldNames, orderedArgs);
        }
        Finalizer finalizer = finalizerMap.get(method);
        return finalizer.doFinalAction(dataset, modelClass, orderedArgs);
    }
}
