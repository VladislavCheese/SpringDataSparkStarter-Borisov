package unsafe.starter.spark.data.lazy.collections;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.context.ConfigurableApplicationContext;
import unsafe.starter.spark.data.annotations.ForeignKeyName;
import unsafe.starter.spark.data.annotations.Source;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.Collection;
import java.util.List;

@RequiredArgsConstructor
public class LazyCollectionSupportPostFinalizer implements PostFinalizer {

    private final ConfigurableApplicationContext context;

    @Override
    @SneakyThrows
    public Object postFinalize(Object retVal) {
        if (Collection.class.isAssignableFrom(retVal.getClass())) {
            List list = (List) retVal;
            for (Object model : list) {

                Field idField = model.getClass().getDeclaredField("id");
                idField.setAccessible(true);
                long ownerId = idField.getLong(model);

                Field[] fields = model.getClass().getDeclaredFields();
                for (Field field : fields) {
                    if (List.class.isAssignableFrom(field.getType())) {
                        LazySparkList sparkList = context.getBean(LazySparkList.class);
                        sparkList.setOwnerId(ownerId);
                        String columnName = field.getAnnotation(ForeignKeyName.class).fieldName();
                        sparkList.setForeignKeyName(columnName);
                        Class<?> embeddedModel = getEmbeddedModel(field);
                        sparkList.setModelClass(embeddedModel);
                        String pathToData = embeddedModel.getAnnotation(Source.class).value();
                        sparkList.setPathToSource(pathToData);
                        field.setAccessible(true);
                        field.set(model, sparkList);
                    }
                }
            }

        }
        return retVal;
    }

    private Class<?> getEmbeddedModel(Field field) {
        ParameterizedType genericType = (ParameterizedType) field.getGenericType();
        Class<?> embeddedModel =(Class<?>) genericType.getActualTypeArguments()[0];
        return embeddedModel;
    }
}
