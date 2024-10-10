use crate::KeyType;

#[derive(Clone, Copy)]
pub(crate) enum KeyProvider {
	OrderInteger(OrderedInteger),
	UnorderedInteger(UnorderedInteger),
}

impl KeyProvider {
	pub(crate) fn new(key_type: KeyType, random: bool) -> Self {
		match key_type {
			KeyType::Integer => {
				if random {
					Self::UnorderedInteger(UnorderedInteger::default())
				} else {
					Self::OrderInteger(OrderedInteger::default())
				}
			}
			KeyType::String16 => {
				todo!()
			}
			KeyType::String68 => {
				todo!()
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

#[derive(Default, Clone, Copy)]
pub(crate) struct OrderedInteger();
impl IntegerKeyProvider for OrderedInteger {
	fn key(&mut self, n: u32) -> u32 {
		n
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
