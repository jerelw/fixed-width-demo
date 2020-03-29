package com.healthesystems.financial;

import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.item.file.transform.FixedLengthTokenizer;
import org.springframework.batch.item.file.transform.LineTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.validation.BindException;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@EnableBatchProcessing
@AllArgsConstructor
public class BatchConfig {	
	
	private JobBuilderFactory jobBuilderFactory;	
	private StepBuilderFactory stepBuilderFactory;		
	
	@Bean
	public ItemReader<Person> personItemReader() {
		FlatFileItemReader<Person> reader = new FlatFileItemReader<Person>();
		reader.setResource(new ClassPathResource("text.txt"));
		//reader.setLinesToSkip(1);
		reader.setLineMapper(personLineMapper());

		return reader;
	}
	
	@Bean
	public LineMapper<Person> personLineMapper() {
		DefaultLineMapper<Person> mapper = new DefaultLineMapper<Person>();
		mapper.setLineTokenizer(personLineTokenizer());		
		mapper.setFieldSetMapper(new FieldSetMapper<Person>() {

			@Override
			public Person mapFieldSet(FieldSet fieldSet) throws BindException {
				Person product = new Person();

				product.setId(fieldSet.readInt("id"));
				product.setName(fieldSet.readString("name"));		
				product.setAmount(fieldSet.readBigDecimal("amount"));

				return product;
			}});
		return mapper;
	}
	
	@Bean
	public LineTokenizer personLineTokenizer() {
		FixedLengthTokenizer tokenizer = new FixedLengthTokenizer();
		tokenizer.setColumns(new Range[] { new Range(1, 10), new Range(11, 30), new Range(31, 35) });
		tokenizer.setNames(new String[] { "id", "name", "amount" });
		return tokenizer;
	}
	
	public ItemWriter<Person> writer() {
		return new ItemWriter<Person>() {
			@Override
			public void write(List<? extends Person> items) throws Exception {
				items.forEach(i -> log.info("Writing {}", i));				
			}};
	}
	
	@Bean
	public Step step() {
		return stepBuilderFactory.get("step").<Person,Person>chunk(10).reader(personItemReader()).writer(writer()).build();
	}
	
	@Bean
	public Job job() {
		return jobBuilderFactory.get("job").preventRestart().incrementer(new RunIdIncrementer()).flow(step()).end().build();
	}

}
