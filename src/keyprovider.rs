use crate::KeyType;
use twox_hash::XxHash64;

#[derive(Clone, Copy)]
pub(crate) enum KeyProvider {
	OrderedInteger(OrderedInteger),
	UnorderedInteger(UnorderedInteger),
	OrderedString(OrderedString),
	UnorderedString(UnorderedString),
}

impl KeyProvider {
	pub(crate) fn new(key_type: KeyType, random: bool) -> Self {
		match key_type {
			KeyType::Integer => {
				if random {
					Self::UnorderedInteger(UnorderedInteger::default())
				} else {
					Self::OrderedInteger(OrderedInteger::default())
				}
			}
			KeyType::String26 => {
				if random {
					Self::UnorderedString(UnorderedString::new(1))
				} else {
					Self::OrderedString(OrderedString::new(1))
				}
			}
			KeyType::String90 => {
				if random {
					Self::UnorderedString(UnorderedString::new(5))
				} else {
					Self::OrderedString(OrderedString::new(5))
				}
			}
			KeyType::String250 => {
				if random {
					Self::UnorderedString(UnorderedString::new(15))
				} else {
					Self::OrderedString(OrderedString::new(15))
				}
			}
			KeyType::String506 => {
				if random {
					Self::UnorderedString(UnorderedString::new(31))
				} else {
					Self::OrderedString(OrderedString::new(31))
				}
			}
			KeyType::Uuid => {
				todo!()
			}
		}
	}
}

pub(crate) trait IntegerKeyProvider {
	fn key(&mut self, n: u32) -> u32;
}

pub(crate) trait StringKeyProvider {
	fn key(&mut self, n: u32) -> String;
}

#[derive(Default, Clone, Copy)]
pub(crate) struct OrderedInteger();

impl IntegerKeyProvider for OrderedInteger {
	fn key(&mut self, n: u32) -> u32 {
		// We need to increment by 1
		// because MySQL PRIMARY IDs
		// can not be 0, resulting in
		// duplicate ID errors.
		n + 1
	}
}
#[derive(Default, Clone, Copy)]
pub(crate) struct UnorderedInteger();

impl IntegerKeyProvider for UnorderedInteger {
	fn key(&mut self, n: u32) -> u32 {
		Self::feistel_transform(n)
	}
}

impl UnorderedInteger {
	// A very simple round function: XOR the input with the key and shift
	fn feistel_round_function(value: u32, key: u32) -> u32 {
		(value ^ key).rotate_left(5).wrapping_add(key)
	}

	// Perform one round of the Feistel network
	fn feistel_round(left: u16, right: u16, round_key: u32) -> (u16, u16) {
		let new_left = right;
		let new_right = left ^ (Self::feistel_round_function(right as u32, round_key) as u16);
		(new_left, new_right)
	}

	fn feistel_transform(input: u32) -> u32 {
		let mut left = (input >> 16) as u16;
		let mut right = (input & 0xFFFF) as u16;

		// Hard-coded keys for simplicity
		let keys = [0xA5A5A5A5, 0x5A5A5A5A, 0x3C3C3C3C];

		for &key in &keys {
			let (new_left, new_right) = Self::feistel_round(left, right, key);
			left = new_left;
			right = new_right;
		}

		// Combine left and right halves back into a single u32
		((left as u32) << 16) | (right as u32)
	}
}

fn hash_string(n: u32, repeat: usize) -> String {
	let mut hex_string = String::with_capacity(repeat * 16 + 10);
	for s in 0..repeat as u64 {
		let hash_result = XxHash64::oneshot(s, &n.to_be_bytes());
		hex_string.push_str(&format!("{hash_result:x}"));
	}
	hex_string
}

#[derive(Clone, Copy)]
pub(crate) struct OrderedString(usize);

impl OrderedString {
	fn new(repeat: usize) -> Self {
		Self(repeat)
	}
}

impl StringKeyProvider for OrderedString {
	fn key(&mut self, n: u32) -> String {
		let hex_string = hash_string(n, self.0);
		format!("{n:010}{hex_string}")
	}
}

#[derive(Default, Clone, Copy)]
pub(crate) struct UnorderedString(usize);

impl UnorderedString {
	fn new(repeat: usize) -> Self {
		Self(repeat)
	}
}

impl StringKeyProvider for UnorderedString {
	fn key(&mut self, n: u32) -> String {
		let hex_string = hash_string(n, self.0);
		format!("{hex_string}{n:010}")
	}
}

#[cfg(test)]
mod test {
	use crate::keyprovider::{OrderedString, StringKeyProvider, UnorderedString};

	#[test]
	fn ordered_string_26() {
		let mut o = OrderedString::new(1);
		let s = o.key(12345678);
		assert_eq!(s.len(), 26);
		assert_eq!(s, "0012345678d79235c904e704c6");
	}

	#[test]
	fn unordered_string_26() {
		let mut o = UnorderedString::new(1);
		let s = o.key(12345678);
		assert_eq!(s.len(), 26);
		assert_eq!(s, "d79235c904e704c60012345678");
	}

	#[test]
	fn ordered_string_90() {
		let mut o = OrderedString::new(5);
		let s = o.key(12345678);
		assert_eq!(s.len(), 90);
		assert_eq!(s, "0012345678d79235c904e704c6c379c25fea98cd11b4d0f71900f91df2ecc87c25d7fff4b03be1bd13590485d3");
	}

	#[test]
	fn unordered_string_90() {
		let mut o = UnorderedString::new(5);
		let s = o.key(12345678);
		assert_eq!(s.len(), 90);
		assert_eq!(s, "d79235c904e704c6c379c25fea98cd11b4d0f71900f91df2ecc87c25d7fff4b03be1bd13590485d30012345678");
	}

	#[test]
	fn ordered_string_250() {
		let mut o = OrderedString::new(15);
		let s = o.key(12345678);
		assert_eq!(s.len(), 250);
		assert_eq!(s, "0012345678d79235c904e704c6c379c25fea98cd11b4d0f71900f91df2ecc87c25d7fff4b03be1bd13590485d31bc0feb2815d5c908f5a4633b8a9d5d6ec1c074d5d64ab296c6495f784f8294ac42b828a9c4ef45d3decc0a8dff00062adfb547fea6132f38afda36acf629cc15413acfe35a50fecbec285e9ee42b136");
	}

	#[test]
	fn unordered_string_250() {
		let mut o = UnorderedString::new(15);
		let s = o.key(12345678);
		assert_eq!(s.len(), 250);
		assert_eq!(s, "d79235c904e704c6c379c25fea98cd11b4d0f71900f91df2ecc87c25d7fff4b03be1bd13590485d31bc0feb2815d5c908f5a4633b8a9d5d6ec1c074d5d64ab296c6495f784f8294ac42b828a9c4ef45d3decc0a8dff00062adfb547fea6132f38afda36acf629cc15413acfe35a50fecbec285e9ee42b1360012345678");
	}

	#[test]
	fn ordered_string_506() {
		let mut o = OrderedString::new(31);
		let s = o.key(12345678);
		assert_eq!(s.len(), 506);
		assert_eq!(s, "0012345678d79235c904e704c6c379c25fea98cd11b4d0f71900f91df2ecc87c25d7fff4b03be1bd13590485d31bc0feb2815d5c908f5a4633b8a9d5d6ec1c074d5d64ab296c6495f784f8294ac42b828a9c4ef45d3decc0a8dff00062adfb547fea6132f38afda36acf629cc15413acfe35a50fecbec285e9ee42b13691088df6c3740c87c3d003e3addf1888a582ac5cb408feec138fe9a43c9fda574006e770bb0b5e84edcbeecc6f723960ed7d02591a7b2487bb317f83bfd95e44a69d957deb6b10e22d895a375acfa54143137feeb53921625bc9d582166477e562454fecc90f130662338c070bd709c27d8478abaa825dc69bc3aa89dc7ce076");
	}

	#[test]
	fn unordered_string_506() {
		let mut o = UnorderedString::new(31);
		let s = o.key(12345678);
		assert_eq!(s.len(), 506);
		assert_eq!(s, "d79235c904e704c6c379c25fea98cd11b4d0f71900f91df2ecc87c25d7fff4b03be1bd13590485d31bc0feb2815d5c908f5a4633b8a9d5d6ec1c074d5d64ab296c6495f784f8294ac42b828a9c4ef45d3decc0a8dff00062adfb547fea6132f38afda36acf629cc15413acfe35a50fecbec285e9ee42b13691088df6c3740c87c3d003e3addf1888a582ac5cb408feec138fe9a43c9fda574006e770bb0b5e84edcbeecc6f723960ed7d02591a7b2487bb317f83bfd95e44a69d957deb6b10e22d895a375acfa54143137feeb53921625bc9d582166477e562454fecc90f130662338c070bd709c27d8478abaa825dc69bc3aa89dc7ce0760012345678");
	}
}
