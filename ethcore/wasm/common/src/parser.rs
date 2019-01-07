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

//! ActionParams parser for wasm

use vm;
use pwasm_utils::{self, rules};
use parity_wasm::elements::{self, Deserialize};
use parity_wasm::peek_size;
use std::io;


/// Splits payload to code and data according to params.params_type, also
/// loads the module instance from payload and injects gas counter according
/// to schedule.
pub fn payload<'a>(
        params: &'a vm::ActionParams,
        costs: &rules::Set,
        max_stack_height: u32
) -> Result<(elements::Module, &'a [u8]), vm::Error> {
	let code = match params.code {
		Some(ref code) => &code[..],
		None => { return Err(vm::Error::Wasm("Invalid wasm call".to_owned())); }
	};

	let (mut cursor, data_position) = match params.params_type {
		vm::ParamsType::Embedded => {
			let module_size = peek_size(&*code);
                        let cursor = io::Cursor::new(&code[..module_size]);
			(cursor, module_size)
		},
		vm::ParamsType::Separate => {
			(io::Cursor::new(&code[..]), 0)
		},
	};

	let deserialized_module = elements::Module::deserialize(&mut cursor)
                .map_err(|err| {
			vm::Error::Wasm(format!("Error deserializing contract code ({:?})", err))
		})?;

	if deserialized_module.memory_section().map_or(false, |ms| ms.entries().len() > 0) {
		// According to WebAssembly spec, internal memory is hidden from embedder and should not
		// be interacted with. So we disable this kind of modules at decoding level.
		return Err(vm::Error::Wasm(format!("Malformed wasm module: internal memory")));
	}

	let contract_module = pwasm_utils::inject_gas_counter(deserialized_module, costs)
                .map_err(|_| vm::Error::Wasm(format!("Wasm contract error: bytecode invalid")))?;

	let contract_module = pwasm_utils::stack_height::inject_limiter(
                contract_module,
		max_stack_height
	).map_err(|_| vm::Error::Wasm(format!("Wasm contract error: stack limiter failure")))?;

	let data = match params.params_type {
		vm::ParamsType::Embedded => {
			if data_position < code.len() { &code[data_position..] } else { &[] }
		},
		vm::ParamsType::Separate => {
			match params.data {
				Some(ref s) => &s[..],
				None => &[]
			}
		}
	};

	Ok((contract_module, data))
}
