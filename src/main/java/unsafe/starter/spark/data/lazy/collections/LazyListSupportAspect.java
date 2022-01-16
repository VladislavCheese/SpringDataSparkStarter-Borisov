package unsafe.starter.spark.data.lazy.collections;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.List;

@Aspect
//аспект является бином
public class LazyListSupportAspect {

    @Autowired
    private ConfigurableApplicationContext context;

    @Autowired
    private FirstLevelCacheService cacheService;

    @Before("execution(* unsafe.starter.spark.data.lazy.collections.LazySparkList.*(..)) && execution(* java.util.*.*(..))")
    public void beforeEachMethodInvocationCheckAndFillContent(JoinPoint jp) {
        LazySparkList lazyList = (LazySparkList) jp.getTarget();
        if (!lazyList.initialized()) {
            List list = cacheService.getDataFor(lazyList.getOwnerId(), lazyList.getForeignKeyName(), lazyList.getModelClass(), lazyList.getPathToSource(), context);
            lazyList.setContent(list);
        }
    }

}
