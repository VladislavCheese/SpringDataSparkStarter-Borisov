package unsafe.starter.spark.data.extractors;

import lombok.AllArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@AllArgsConstructor
public class DataExtractorResolver {

    private Map<String, DataExtractor> extractorMap;

    public DataExtractor resolve(String pathToData) {
        String[] fileExtentions = pathToData.split("\\.");
        if (fileExtentions.length != 2) {
            throw new RuntimeException("Не указано расширение файла с данными");
        }
        return extractorMap.get(fileExtentions[1]);
    }
}
