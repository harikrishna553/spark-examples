package com.sample.app.model;

import java.io.Serializable;

public class Employee implements Serializable {

	private static final long serialVersionUID = 1878126L;
	
	private int id;
	private String firstName;

	public Employee(int id, String firstName) {
		super();
		this.id = id;
		this.firstName = firstName;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	@Override
	public String toString() {
		return "Employee [id=" + id + ", firstName=" + firstName + "]";
	}

}
