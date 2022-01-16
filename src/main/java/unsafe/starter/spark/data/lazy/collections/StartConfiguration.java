package unsafe.starter.spark.data.lazy.collections;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class StartConfiguration {

    @Bean
    @Scope("prototype")
    public LazySparkList lazySparkList() {
        return new LazySparkList();
    }
    @Bean
    public FirstLevelCacheService firstLevelCacheService(){
        return new FirstLevelCacheService();
    }
    @Bean
    public LazyListSupportAspect lazyListSupportAspect(){
        return new LazyListSupportAspect();
    }
}
