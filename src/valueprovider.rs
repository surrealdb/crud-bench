use anyhow::{bail, Result};
use fake::Fake;
use log::debug;
use rand::prelude::SmallRng;
use rand::SeedableRng;
use serde_json::{Map, Number, Value};
use std::str::FromStr;

#[derive(Clone)]
pub(crate) struct ValueProvider {
	val: Value,
	rng: SmallRng,
	columns: Columns,
}

#[derive(Clone)]
pub(crate) struct Columns(pub(crate) Vec<(String, ColumnType)>);

#[derive(Clone)]
pub(crate) enum ColumnType {
	String,
	Integer,
	Object,
}

impl ColumnType {
	fn to_sql_string(&self, val: &Value) -> String {
		match self {
			Self::String => format!("'{}'", val.as_str().unwrap()),
			Self::Object => {
				format!("'{}'", serde_json::to_string(val.as_object().unwrap()).unwrap())
			}
			Self::Integer => val.to_string(),
		}
	}
}

impl Columns {
	pub(crate) fn insert_clauses(&self, val: Value) -> Result<(String, String)> {
		let val = val.as_object().unwrap();
		let mut fields = Vec::with_capacity(self.0.len());
		let mut values = Vec::with_capacity(self.0.len());
		for (n, t) in &self.0 {
			fields.push(n.to_string());
			let value = t.to_sql_string(val.get(n).unwrap());
			values.push(value);
		}
		let fields = fields.join(",");
		let values = values.join(",");
		Ok((fields, values))
	}

	pub(crate) fn set_clause(&self, val: Value) -> Result<String> {
		let mut updates = Vec::with_capacity(self.0.len());
		let val = val.as_object().unwrap();
		for (n, t) in &self.0 {
			let value = t.to_sql_string(val.get(n).unwrap());
			updates.push(format!("{n}={value}"));
		}
		Ok(updates.join(","))
	}
}

impl ValueProvider {
	pub(crate) fn new(json: &str) -> Result<Self> {
		let val = serde_json::from_str(json)?;
		debug!("Value template: {val:#}");
		let columns = Self::parse_columns(&val)?;
		Ok(Self {
			val,
			columns,
			rng: SmallRng::from_entropy(),
		})
	}

	fn parse_columns(val: &Value) -> Result<Columns> {
		let o = val.as_object().unwrap();
		let mut columns = Vec::with_capacity(o.len());
		for (f, t) in o {
			// Arrays
			if t.is_object() {
				columns.push((f.to_string(), ColumnType::Object));
			} else if let Some(str) = t.as_str() {
				let str = str.to_ascii_lowercase();
				if str.starts_with("@fake:bool") {
					todo!()
				} else if str.starts_with("@fake:int") {
					columns.push((f.to_string(), ColumnType::Integer));
				} else if str.starts_with("@fake:float") {
					todo!()
				} else if str.starts_with("@fake:string")
					|| str.starts_with("@fake:word")
					|| str.starts_with("@fake:sentence")
					|| str.starts_with("@fake:paragraph")
					|| str.starts_with("@fake:string")
					|| str.starts_with("@fake:name")
					|| str.starts_with("@fake:firstname")
					|| str.starts_with("@fake:lastname")
					|| str.starts_with("@fake:address1")
					|| str.starts_with("@fake:address2")
					|| str.starts_with("@fake:city")
					|| str.starts_with("@fake:state")
					|| str.starts_with("@fake:country")
					|| str.starts_with("@fake:countrycode")
					|| str.starts_with("@fake:postcode")
					|| str.starts_with("@fake:zipcode")
					|| str.starts_with("@fake:latitude")
					|| str.starts_with("@fake:longitude")
				{
					columns.push((f.to_string(), ColumnType::String));
				} else {
					bail!("Invalid data type: {str}");
				}
			} else {
				bail!("Invalid data type: {t}");
			}
		}
		Ok(Columns(columns))
	}

	pub(crate) fn columns(&self) -> Columns {
		self.columns.clone()
	}

	pub(crate) fn generate_value(&mut self) -> Result<Value> {
		Self::parse_value(&mut self.rng, &self.val)
	}
	fn parse_value(rng: &mut SmallRng, value: &Value) -> Result<Value> {
		// Generate any object structure values
		if let Some(object) = value.as_object() {
			let mut map = Map::<String, Value>::new();
			for (key, value) in object {
				map.insert(key.clone(), Self::parse_value(rng, value)?);
			}
			Ok(Value::Object(map))
		}
		// Generate any array structure values
		else if let Some(array) = value.as_array() {
			let mut vec = Vec::with_capacity(array.len());
			for value in array {
				vec.push(Self::parse_value(rng, value)?);
			}
			Ok(Value::Array(vec))
		}
		// Process and generate any fake data
		else if let Some(str) = value.as_str() {
			// Check if this is a fake string
			if let Some(input) = str.strip_prefix("@fake:") {
				// Get the fake data type
				let kind: Vec<&str> = input.splitn(2, ":").collect();
				// Check if a type was set
				if kind.is_empty() {
					bail!("Invalid fake type: {str}");
				}
				// This is a
				match kind[0] {
					//
					"bool" => {
						let val = fake::Faker.fake::<bool>();
						Ok(Value::Bool(val))
					}
					//
					"int" => match kind.len() {
						1 => {
							let val = fake::Faker.fake::<i64>();
							Ok(Value::Number(val.into()))
						}
						2 => {
							// Get the length config setting
							let gen: Vec<&str> = kind[1].split("..").collect();
							// Check the length parameter
							match gen.len() {
								2 => {
									let min = i64::from_str(gen[0])?;
									let max = i64::from_str(gen[1])?;
									let val = (min..max).fake::<i64>();
									Ok(Value::Number(val.into()))
								}
								v => {
									bail!("Invalid length generation value: {v}");
								}
							}
						}
						_ => {
							bail!("Expected a length configuration value");
						}
					},
					"float" => match kind.len() {
						1 => {
							let val = fake::Faker.fake::<f64>();
							Ok(Value::Number(Number::from_f64(val).unwrap()))
						}
						2 => {
							// Get the length config setting
							let gen: Vec<&str> = kind[1].split("..").collect();
							// Check the length parameter
							match gen.len() {
								2 => {
									let min = f64::from_str(gen[0])?;
									let max = f64::from_str(gen[1])?;
									let val = (min..max).fake::<f64>();
									Ok(Value::Number(Number::from_f64(val).unwrap()))
								}
								v => {
									bail!("Invalid length generation value: {v}");
								}
							}
						}
						_ => {
							bail!("Expected a length configuration value");
						}
					},
					//
					"string" => match kind.len() {
						1 => {
							let val = fake::Faker.fake::<String>();
							Ok(Value::String(val))
						}
						2 => {
							// Get the length config setting
							let gen: Vec<&str> = kind[1].split("..").collect();
							// Check the length parameter
							match gen.len() {
								2 => {
									let min = usize::from_str(gen[0])?;
									let max = usize::from_str(gen[1])?;
									let val = (min..max).fake::<String>();
									Ok(Value::String(val))
								}
								1 => {
									let len = usize::from_str(gen[0])?;
									let val = (len).fake::<String>();
									Ok(Value::String(val))
								}
								v => {
									bail!("Invalid length generation value: {v}");
								}
							}
						}
						_ => {
							bail!("Expected a length configuration value");
						}
					},
					"paragraph" => match kind.len() {
						2 => {
							// Get the length config setting
							let gen: Vec<&str> = kind[1].split("..").collect();
							// Check the length parameter
							match gen.len() {
								2 => {
									let min = usize::from_str(gen[0])?;
									let max = usize::from_str(gen[1])?;
									let val: String =
										fake::faker::lorem::en::Paragraph(min..max).fake();
									Ok(Value::String(val))
								}
								1 => {
									let len = usize::from_str(gen[0])?;
									let val: String =
										fake::faker::lorem::en::Paragraph(len..len).fake();
									Ok(Value::String(val))
								}
								v => {
									bail!("Invalid length generation value: {v}");
								}
							}
						}
						_ => {
							bail!("Expected a length configuration value");
						}
					},
					"sentence" => match kind.len() {
						2 => {
							// Get the length config setting
							let gen: Vec<&str> = kind[1].split("..").collect();
							// Check the length parameter
							match gen.len() {
								2 => {
									let min = usize::from_str(gen[0])?;
									let max = usize::from_str(gen[1])?;
									let val: String =
										fake::faker::lorem::en::Sentence(min..max).fake();
									Ok(Value::String(val))
								}
								1 => {
									let len = usize::from_str(gen[0])?;
									let val: String =
										fake::faker::lorem::en::Sentence(len..len).fake();
									Ok(Value::String(val))
								}
								v => {
									bail!("Invalid length generation value: {v}");
								}
							}
						}
						_ => {
							bail!("Expected a length configuration value");
						}
					},
					"word" => match kind.len() {
						1 => {
							let val: String = fake::faker::lorem::en::Word().fake();
							Ok(Value::String(val))
						}
						2 => {
							// Get the length config setting
							let gen: Vec<&str> = kind[1].split("..").collect();
							// Check the length parameter
							match gen.len() {
								2 => {
									let min = usize::from_str(gen[0])?;
									let max = usize::from_str(gen[1])?;
									let val: Vec<String> =
										fake::faker::lorem::en::Words(min..max).fake();
									Ok(Value::String(val.join(" ")))
								}
								1 => {
									let len = usize::from_str(gen[0])?;
									let val: Vec<String> =
										fake::faker::lorem::en::Words(len..len).fake();
									Ok(Value::String(val.join(" ")))
								}
								v => {
									bail!("Invalid length generation value: {v}");
								}
							}
						}
						_ => {
							bail!("Expected a length configuration value");
						}
					},
					//
					"name" => {
						let val: String = fake::faker::name::en::Name().fake();
						Ok(Value::String(val))
					}
					"firstname" => {
						let val: String = fake::faker::name::en::FirstName().fake();
						Ok(Value::String(val))
					}
					"lastname" => {
						let val: String = fake::faker::name::en::LastName().fake();
						Ok(Value::String(val))
					}
					//
					"address1" => {
						let val: String = fake::faker::address::en::StreetName().fake();
						Ok(Value::String(val))
					}
					"address2" => {
						let val: String = fake::faker::address::en::SecondaryAddress().fake();
						Ok(Value::String(val))
					}
					"city" => {
						let val: String = fake::faker::address::en::CityName().fake();
						Ok(Value::String(val))
					}
					"state" => {
						let val: String = fake::faker::address::en::StateName().fake();
						Ok(Value::String(val))
					}
					"country" => {
						let val: String = fake::faker::address::en::CountryName().fake();
						Ok(Value::String(val))
					}
					"countrycode" => {
						let val: String = fake::faker::address::en::CountryCode().fake();
						Ok(Value::String(val))
					}
					"postcode" => {
						let val: String = fake::faker::address::en::PostCode().fake();
						Ok(Value::String(val))
					}
					"zipcode" => {
						let val: String = fake::faker::address::en::ZipCode().fake();
						Ok(Value::String(val))
					}
					"latitude" => {
						let val: String = fake::faker::address::en::Latitude().fake();
						Ok(Value::String(val))
					}
					"longitude" => {
						let val: String = fake::faker::address::en::Longitude().fake();
						Ok(Value::String(val))
					}
					//
					_ => bail!("Invalid fake type: {str}"),
				}
			} else {
				Ok(Value::String(str.into()))
			}
		}
		// We found an invalid data type
		else {
			bail!("Invalid data type: {value}");
		}
	}
}
