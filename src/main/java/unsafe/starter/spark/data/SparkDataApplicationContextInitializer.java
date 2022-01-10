package unsafe.starter.spark.data;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import unsafe.starter.spark.data.api.SparkRepository;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.reflections.Reflections;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;

import java.beans.Introspector;
import java.lang.reflect.Proxy;

public class SparkDataApplicationContextInitializer implements ApplicationContextInitializer {
    @Override
    public void initialize(ConfigurableApplicationContext context) {
        //поднимем временный контекст для настройки бинов
        AnnotationConfigApplicationContext tempContext = new AnnotationConfigApplicationContext(InternalConfiguration.class);
        SparkInvocationHandlerFactory factory = tempContext.getBean(SparkInvocationHandlerFactory.class);
        tempContext.close();
        factory.setContext(context);

        registerSparkBeans(context);
        Reflections scanner = new Reflections(context.getEnvironment().getProperty("spark.package-to-scan"));

        scanner.getSubTypesOf(SparkRepository.class).forEach(sparkRepositoryInterface -> {
            Object poxy = Proxy.newProxyInstance(sparkRepositoryInterface.getClassLoader(),
                    new Class[]{sparkRepositoryInterface},
                    factory.create(sparkRepositoryInterface));
            context.getBeanFactory().registerSingleton(Introspector.decapitalize(sparkRepositoryInterface.getSimpleName()), poxy);
        });
    }

    private void registerSparkBeans(ConfigurableApplicationContext context) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName(context.getEnvironment().getProperty("spark.app-name"))
                .getOrCreate();
        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());
        context.getBeanFactory().registerSingleton("sparkSession",sparkSession);
        context.getBeanFactory().registerSingleton("sparkContext",sparkContext);
    }
}
