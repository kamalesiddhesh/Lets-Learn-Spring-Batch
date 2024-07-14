package in.api.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
//import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import in.api.entity.Customer;
import in.api.repo.CustomerRepository;

@Configuration
//@EnableBatchProcessing
public class CsvBatchConfig {
	
	@Autowired
	private CustomerRepository repo;
	
	//create Reader
	@Bean
	public FlatFileItemReader<Customer> customerReader(){
		FlatFileItemReader<Customer> itemReader = new FlatFileItemReader<>();
		itemReader.setResource(new FileSystemResource("src/main/resources/customers.csv"));
		itemReader.setName("csv-reader");
		itemReader.setLinesToSkip(1);
		itemReader.setLineMapper(lineMapper());
		
		
		return itemReader;
	}
	
	private LineMapper<Customer> lineMapper(){
		DefaultLineMapper<Customer> lineMapper= new DefaultLineMapper<>();
		DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
		lineTokenizer.setDelimiter(",");
		lineTokenizer.setStrict(false);
		lineTokenizer.setNames("id","firstName","lastName","email","gender","contactNo","country","dob");
		
		BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>(); // convert csv data into java object
		fieldSetMapper.setTargetType(Customer.class);
		
		lineMapper.setLineTokenizer(lineTokenizer);
		lineMapper.setFieldSetMapper(fieldSetMapper);
		return lineMapper;
	}
	
	//create Processor
	@Bean
	public CustomerProcessor customerProcessor() {
		return new CustomerProcessor();
	}
	
	//create Writer
	@Bean
	public RepositoryItemWriter<Customer> customerWriter(){
		RepositoryItemWriter<Customer> repositoryItemWriter = new RepositoryItemWriter<>();
		repositoryItemWriter.setRepository(repo);
		repositoryItemWriter.setMethodName("save");
		
		return repositoryItemWriter;
		
	}
	
	//create Step
	
	@Bean
	public Step customerStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
	    return new StepBuilder("customerStep",jobRepository)
	            .<Customer, Customer>chunk(10, transactionManager) // Process 10 records at a time
	            .reader(customerReader())
	            .processor(customerProcessor())
	            .writer(customerWriter())
				.taskExecutor(taskExecutor()) 
	            .build();
		
		
	}
	
	@Bean
    public TaskExecutor taskExecutor() {
        return new SimpleAsyncTaskExecutor();
    }
	
	
	//create Job
	
	@Bean
	public Job job( JobRepository jobRepository, PlatformTransactionManager transactionManager) {
	    return new JobBuilder("myjob", jobRepository)
	            .flow(customerStep(jobRepository,transactionManager))
	            .end()
	            .build();
	}
	

}
