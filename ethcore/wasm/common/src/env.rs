// Copyright 2015-2018 Parity Technologies (UK) Ltd.
// This file is part of Parity.

// Parity is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Parity is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Parity.  If not, see <http://www.gnu.org/licenses/>.


use wasmi::{self, FuncRef, FuncInstance, Error, ValueType,
            MemoryDescriptor, MemoryRef, MemoryInstance, memory_units};


pub struct StaticSignature(pub &'static [ValueType], pub Option<ValueType>);


impl Into<wasmi::Signature> for StaticSignature {
	fn into(self) -> wasmi::Signature {
		wasmi::Signature::new(self.0, self.1)
	}
}


pub fn alloc_func(signature: StaticSignature, idx: usize) -> FuncRef {
	FuncInstance::alloc_host(signature.into(), idx)
}


pub fn alloc_empty_memory() -> MemoryRef {
        MemoryInstance::alloc(memory_units::Pages(0), Some(memory_units::Pages(0)))
                .expect("Memory allocation (0, 0) should not fail; qed")
}

pub fn alloc_memory(
        descriptor: &MemoryDescriptor,
        max_memory: u32
) -> Result<MemoryRef, Error> {
	let effective_max = descriptor.maximum().unwrap_or(max_memory + 1);
	if descriptor.initial() > max_memory || effective_max > max_memory {
		Err(Error::Instantiation("Module requested too much memory".to_owned()))
	} else {
                let units = memory_units::Pages(descriptor.initial() as usize);
                let max_units = descriptor.maximum().map(|x| memory_units::Pages(x as usize));
                Ok(MemoryInstance::alloc(units, max_units)?)
	}
}
