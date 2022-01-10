package unsafe.starter.spark.data;

import java.beans.Introspector;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class WordsMatcher {
    public static String findAndRemoveMatchingPiecesIfExists(Set<String> options, List<String> pieces) {
        //берем первый фрагмент и удаляем его
        StringBuilder match = new StringBuilder(pieces.remove(0));
        //оставляем только те опции которые начинаются как первый фрагмент
        List<String> remainingOptions = options.stream()
                .filter(option -> option.toLowerCase().startsWith(match.toString().toLowerCase()))
                .collect(Collectors.toList());
        //если не нашлось таких возвращаем пустую строку (она будет означтаь что это был фрагмент And)
        if (remainingOptions.isEmpty()) {
            return "";
        }
        //пока больше одной опции подходит
        while (remainingOptions.size() > 1) {
            //берем следующий фрагмент
            match.append(pieces.remove(0));
            //если каки-то опции больше не подходят - удаляем их
            remainingOptions.removeIf(option -> !option.toLowerCase().startsWith(match.toString().toLowerCase()));
        }
        //пока оставшаяся опция не совпадает польностью с фрагментом
        while (!remainingOptions.get(0).equalsIgnoreCase(match.toString())){
            //удаляем очередной фрагмент и добавляем его к результату
            match.append(pieces.remove(0));
        }
        return Introspector.decapitalize(match.toString());
    }
}
