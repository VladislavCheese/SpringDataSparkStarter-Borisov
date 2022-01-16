package unsafe.starter.spark.data.ih;

import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;
import scala.Tuple2;
import unsafe.starter.spark.data.WordsMatcher;
import unsafe.starter.spark.data.annotations.Source;
import unsafe.starter.spark.data.annotations.Transient;
import unsafe.starter.spark.data.api.SparkRepository;
import unsafe.starter.spark.data.extractors.DataExtractor;
import unsafe.starter.spark.data.extractors.DataExtractorResolver;
import unsafe.starter.spark.data.finalizers.Finalizer;
import unsafe.starter.spark.data.lazy.collections.LazyCollectionSupportPostFinalizer;
import unsafe.starter.spark.data.spiders.TransformationSpider;
import unsafe.starter.spark.data.transformations.SparkTransformation;

import java.beans.Introspector;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

@Component
@RequiredArgsConstructor
public class SparkInvocationHandlerFactory {
    private final DataExtractorResolver extractorResolver;
    private final Map<String, Finalizer> finalizerMap;
    private final Map<String, TransformationSpider> spiderMap;

    @Setter
    private ConfigurableApplicationContext context;

    public SparkInvocationHandler create(Class<? extends SparkRepository> repoInterface) {
        //достанем класс дженерика
        Class<?> modelClass = getModelClass(repoInterface);
        //определим источник данных и получим для него extractor
        String pathToData = modelClass.getAnnotation(Source.class).value();
        DataExtractor dataExtractor = extractorResolver.resolve(pathToData);

        Set<String> fieldNames = getModelFieldNames(modelClass);

        Map<Method, List<Tuple2<SparkTransformation,List<String>>>> transformationChain = new HashMap<>();
        Map<Method, Finalizer> method2Finalizer = new HashMap<>();
        for (Method method : repoInterface.getMethods()) {
            TransformationSpider currentSpider = null;
            List<Tuple2<SparkTransformation,List<String>>> transformations = new ArrayList<>();

            List<String> methodWords = new LinkedList<>(asList(method.getName().split("(?=\\p{Upper})")));
            //если в списке одно слово - то это finalizer и мы не итерируемся
            while (methodWords.size() > 1) {
                String strategyName = WordsMatcher.findAndRemoveMatchingPiecesIfExists(spiderMap.keySet(), methodWords);
                //если пришла пустая строка значит это слово And/Or и стратегия не меняется
                if (!strategyName.isEmpty()) {
                    currentSpider = spiderMap.get(strategyName);
                }
                assert currentSpider != null;
                transformations.add(currentSpider.createTransformation(methodWords, fieldNames));
            }
            transformationChain.put(method, transformations);


            String finalizerName = "collect";
            //если слов не останется значит это дефолтный collect
            if (methodWords.size() == 1) {
                finalizerName = Introspector.decapitalize(methodWords.get(0));
            }
            Finalizer finalizer = finalizerMap.get(finalizerName);
            method2Finalizer.put(method, finalizer);
        }

        return SparkInvocationHandlerImpl.builder()
                .modelClass(modelClass)
                .pathToData(pathToData)
                .dataExtractor(dataExtractor)
                .transformationChain(transformationChain)
                .finalizerMap(method2Finalizer)
                .postFinalizer(new LazyCollectionSupportPostFinalizer(context))
                .context(context)
                .build();
    }

    private Set<String> getModelFieldNames(Class<?> modelClass) {
        return Arrays.stream(modelClass.getDeclaredFields())
                .filter(field -> !field.isAnnotationPresent(Transient.class))
                .filter(field -> !Collections.class.isAssignableFrom(field.getType()))
                .filter(field -> !Map.class.isAssignableFrom(field.getType()))
                .map(Field::getName)
                .collect(Collectors.toSet());
    }

    private Class<?> getModelClass(Class<? extends SparkRepository> repoInterface) {
        ParameterizedType genericInterface = (ParameterizedType) repoInterface.getGenericInterfaces()[0];
        return (Class<?>) genericInterface.getActualTypeArguments()[0];
    }
}
