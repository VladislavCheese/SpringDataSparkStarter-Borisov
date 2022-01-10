package unsafe.starter.spark.data.finalizers;

import lombok.SneakyThrows;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.springframework.stereotype.Component;
import unsafe.starter.spark.data.filters.impl.OrderedBag;

import javax.lang.model.type.ArrayType;
import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Component("collect")
public class CollectFinalizer implements Finalizer {
    @Override
    @SneakyThrows
    public Object doFinalAction(Dataset<Row> dataset, Class<?> modelClass, OrderedBag<Object> args) {
        Encoder<?> encoder = Encoders.bean(modelClass);
        List<String> listFieldNames = Arrays.stream(encoder.schema().fields()).filter(structField -> structField.dataType() instanceof ArrayType)
                .map(StructField::name)
                .collect(Collectors.toList());
        for (String fieldName : listFieldNames) {
            //для всех коллекций проставляем null потому что мы будем грузить их отдельно
            ParameterizedType collectionGenericType = (ParameterizedType) modelClass.getDeclaredField(fieldName).getGenericType();
            Class c = (Class) collectionGenericType.getActualTypeArguments()[0];
            dataset.withColumn(fieldName, functions.lit(null).cast(DataTypes.createStructType(Encoders.bean(c).schema().fields())));
        }

        return dataset.as(encoder).collectAsList();
    }
}
