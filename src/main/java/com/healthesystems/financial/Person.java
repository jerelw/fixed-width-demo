package com.healthesystems.financial;

import java.math.BigDecimal;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Person {
	
	private Integer id;
	private String name;
	private BigDecimal amount;

}
