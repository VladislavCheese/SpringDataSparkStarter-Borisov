package unsafe.starter.spark.data;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan
//@ConditionalOnProperty(prefix = "spark.package-to-scan")
public class InternalConfiguration {
}
