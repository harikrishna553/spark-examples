package com.sample.app.demo;

import java.util.regex.Pattern;

public class App {
	public static void main(String args[]) {
		System.out.println(Pattern.matches(".*health", "user/v2/users/health"));
	}

}
