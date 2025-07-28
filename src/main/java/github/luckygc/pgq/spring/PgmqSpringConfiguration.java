package github.luckygc.pgq.spring;

import github.luckygc.pgq.api.QueueManager;
import github.luckygc.pgq.impl.QueueManagerImpl;
import javax.sql.DataSource;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.SQLExceptionTranslator;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;


public class PgmqSpringConfiguration {

    @Bean
    public QueueManager queueManager(DataSource dataSource, PlatformTransactionManager platformTransactionManager,
            ObjectProvider<SQLExceptionTranslator> sqlExceptionTranslator) {
        JdbcTemplate jdbcTemplate = createJt(dataSource, sqlExceptionTranslator);
        TransactionTemplate transactionTemplate = new TransactionTemplate(platformTransactionManager);
        return new QueueManagerImpl(jdbcTemplate, transactionTemplate);
    }

    private JdbcTemplate createJt(DataSource dataSource,
            ObjectProvider<SQLExceptionTranslator> sqlExceptionTranslator) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.setIgnoreWarnings(true);
        jdbcTemplate.setFetchSize(-1);
        jdbcTemplate.setMaxRows(-1);
        jdbcTemplate.setSkipResultsProcessing(false);
        jdbcTemplate.setSkipUndeclaredResults(false);
        jdbcTemplate.setResultsMapCaseInsensitive(false);
        sqlExceptionTranslator.ifUnique(jdbcTemplate::setExceptionTranslator);
        return jdbcTemplate;
    }
}
