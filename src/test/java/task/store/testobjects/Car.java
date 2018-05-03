package task.store.testobjects;

import java.io.Serializable;

public class Car implements Serializable {
	
	public String brand;
	public String model;
	public int year;

	public Car(String brand, String model, int year) {
		this.brand = brand;
		this.model = model;
		this.year = year;
	}

	@Override
	public String toString() {
		return "Car [brand=" + brand + ", model=" + model + ", year=" + year
				+ "]";
	}

}