package github.luckygc.pgq.spring;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.context.annotation.Import;

/**
 * 启用PGMQ功能的注解。
 * 当在配置类上使用此注解时，会自动导入PGMQ相关的Spring配置。
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(PgmqSpringConfiguration.class)
public @interface EnablePgmq {

}
