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

//! Env module glue for wasmi interpreter

use std::cell::RefCell;
use wasmi::{self, Signature, Error, FuncRef, MemoryDescriptor, MemoryRef};

use wasm_exec_common::env as wasm_env;

/// Internal ids all functions runtime supports. This is just a glue for wasmi interpreter
/// that lacks high-level api and later will be factored out
pub mod ids {
	pub const USE_GAS_FUNC: usize = 0;
	pub const GET_GAS_LEFT_FUNC: usize = 10;
	pub const GET_ADDRESS_FUNC: usize = 20;
	pub const GET_EXTERNAL_BALANCE_FUNC: usize = 30;
	pub const GET_BLOCK_COINBASE_FUNC: usize = 40;
	pub const GET_BLOCK_DIFFICULTY_FUNC: usize = 50;
	pub const GET_BLOCK_GAS_LIMIT_FUNC: usize = 60;
	pub const GET_BLOCK_HASH_FUNC: usize = 70;
	pub const GET_BLOCK_NUMBER_FUNC: usize = 80;
	pub const GET_BLOCK_TIMESTAMP_FUNC: usize = 90;
	pub const GET_TX_GAS_PRICE_FUNC: usize = 100;
	pub const GET_TX_ORIGIN_FUNC: usize = 110;
	pub const LOG_FUNC: usize = 120;
	pub const CALL_FUNC: usize = 130;
	pub const CALL_CODE_FUNC: usize = 140;
	pub const CALL_DELEGATE_FUNC: usize = 150;
	pub const CALL_STATIC_FUNC: usize = 160;
	pub const CREATE_FUNC: usize = 170;
	pub const RETURN_DATA_COPY_FUNC: usize = 180;
	pub const GET_RETURN_DATA_SIZE_FUNC: usize = 190;
	pub const FINISH_FUNC: usize = 200;
	pub const REVERT_FUNC: usize = 210;
	pub const CALL_DATA_COPY_FUNC: usize = 220;
	pub const GET_CALL_DATA_SIZE_FUNC: usize = 230;
	pub const GET_CALLER_FUNC: usize = 240;
	pub const GET_CALL_VALUE_FUNC: usize = 250;
	pub const CODE_COPY_FUNC: usize = 260;
	pub const GET_CODE_SIZE_FUNC: usize = 270;
	pub const EXTERNAL_CODE_COPY_FUNC: usize = 280;
	pub const GET_EXTERNAL_CODE_SIZE_FUNC: usize = 290;
	pub const STORAGE_LOAD_FUNC: usize = 300;
	pub const STORAGE_STORE_FUNC: usize = 310;
	pub const SELF_DESTRUCT_FUNC: usize = 320;
}

/// Signatures of all functions runtime supports. The actual dispatch happens at
/// impl runtime::Runtime methods.
pub mod signatures {
	use wasmi::ValueType::*;
        use super::wasm_env::StaticSignature;

	pub const USE_GAS: StaticSignature = StaticSignature(
		&[I64],
		None
	);
	pub const GET_GAS_LEFT: StaticSignature = StaticSignature(
		&[],
		Some(I64)
	);
	pub const GET_ADDRESS: StaticSignature = StaticSignature(
		&[I32],
		None
	);
	pub const GET_EXTERNAL_BALANCE: StaticSignature = StaticSignature(
		&[I32, I32],
		None
	);
	pub const GET_BLOCK_COINBASE: StaticSignature = StaticSignature(
		&[I32],
		None
	);
	pub const GET_BLOCK_DIFFICULTY: StaticSignature = StaticSignature(
		&[I32],
		None
	);
	pub const GET_BLOCK_GAS_LIMIT: StaticSignature = StaticSignature(
		&[],
		Some(I64)
	);
	pub const GET_BLOCK_HASH: StaticSignature = StaticSignature(
		&[I64, I32],
		Some(I32)
	);
	pub const GET_BLOCK_NUMBER: StaticSignature = StaticSignature(
		&[],
		Some(I64)
	);
	pub const GET_BLOCK_TIMESTAMP: StaticSignature = StaticSignature(
		&[],
		Some(I64)
	);
	pub const GET_TX_GAS_PRICE: StaticSignature = StaticSignature(
		&[I32],
		None
	);
	pub const GET_TX_ORIGIN: StaticSignature = StaticSignature(
		&[I32],
		None
	);
	pub const LOG: StaticSignature = StaticSignature(
		&[I32, I32, I32, I32, I32, I32, I32],
		None
	);
	pub const CALL: StaticSignature = StaticSignature(
		&[I64, I32, I32, I32, I32],
		Some(I32)
	);
	pub const CALL_CODE: StaticSignature = StaticSignature(
		&[I64, I32, I32, I32, I32],
		Some(I32)
	);
	pub const CALL_DELEGATE: StaticSignature = StaticSignature(
		&[I64, I32, I32, I32],
		Some(I32)
	);
	pub const CALL_STATIC: StaticSignature = StaticSignature(
		&[I64, I32, I32, I32],
		Some(I32)
	);
	pub const CREATE: StaticSignature = StaticSignature(
		&[I32, I32, I32, I32],
		Some(I32)
	);
	pub const RETURN_DATA_COPY: StaticSignature = StaticSignature(
		&[I32, I32, I32],
		None
	);
	pub const GET_RETURN_DATA_SIZE: StaticSignature = StaticSignature(
		&[],
		Some(I32)
	);
	pub const FINISH: StaticSignature = StaticSignature(
		&[I32, I32],
		None
	);
	pub const REVERT: StaticSignature = StaticSignature(
		&[I32, I32],
		None
	);
	pub const CALL_DATA_COPY: StaticSignature = StaticSignature(
		&[I32, I32, I32],
		None
	);
	pub const GET_CALL_DATA_SIZE: StaticSignature = StaticSignature(
		&[],
		Some(I32)
	);
	pub const GET_CALLER: StaticSignature = StaticSignature(
		&[I32],
		None
	);
	pub const GET_CALL_VALUE: StaticSignature = StaticSignature(
		&[I32],
		None
	);
	pub const CODE_COPY: StaticSignature = StaticSignature(
		&[I32, I32, I32],
		None
	);
	pub const GET_CODE_SIZE: StaticSignature = StaticSignature(
		&[],
		Some(I32)
	);
	pub const EXTERNAL_CODE_COPY: StaticSignature = StaticSignature(
		&[I32, I32, I32, I32],
		None
	);
	pub const GET_EXTERNAL_CODE_SIZE: StaticSignature = StaticSignature(
		&[I32],
		Some(I32)
	);
	pub const STORAGE_LOAD: StaticSignature = StaticSignature(
		&[I32, I32],
		None
	);
	pub const STORAGE_STORE: StaticSignature = StaticSignature(
		&[I32, I32],
		None
	);
	pub const SELF_DESTRUCT: StaticSignature = StaticSignature(
		&[I32],
		None
	);

}

/// Import resolver for wasmi
/// Maps all functions that runtime support to the corresponding contract import
/// entries.
/// Also manages initial memory request from the runtime.
pub struct ImportResolver {
	max_memory: u32,
	memory: RefCell<Option<MemoryRef>>
}


impl ImportResolver {
	/// New import resolver with specifed maximum amount of inital memory (in wasm pages = 64kb)
	pub fn with_limit(max_memory: u32) -> ImportResolver {
		ImportResolver {
			max_memory: max_memory,
			memory: RefCell::new(None)
		}
	}

	/// Returns memory that was instantiated during the contract module
	/// start. If contract does not use memory at all, the dummy memory of length (0, 0)
	/// will be created instead. So this method always returns memory instance
	/// unless errored.
	pub fn memory_ref(&self) -> MemoryRef {
		{
			let mut mem_ref = self.memory.borrow_mut();
			if mem_ref.is_none() {
				*mem_ref = Some(wasm_env::alloc_empty_memory());
			}
		}

		self.memory.borrow().clone().expect("it is either existed or was created as (0, 0) above; qed")
	}

	/// Returns memory size module initially requested
	pub fn memory_size(&self) -> Result<u32, Error> {
		Ok(self.memory_ref().current_size().0 as u32)
	}
}


impl wasmi::ModuleImportResolver for ImportResolver {

	fn resolve_func(
		&self,
		field_name: &str,
		_signature: &Signature
	) -> std::result::Result<FuncRef, Error> {
                use self::wasm_env::alloc_func as host;

		let func_ref = match field_name {
			"useGas" => host(signatures::USE_GAS, ids::USE_GAS_FUNC),
			"getGasLeft" => host(signatures::GET_GAS_LEFT, ids::GET_GAS_LEFT_FUNC),
			"getAddress" => host(signatures::GET_ADDRESS, ids::GET_ADDRESS_FUNC),
			"getExternalBalance" => host(signatures::GET_EXTERNAL_BALANCE, ids::GET_EXTERNAL_BALANCE_FUNC),
			"getBlockCoinbase" => host(signatures::GET_BLOCK_COINBASE, ids::GET_BLOCK_COINBASE_FUNC),
			"getBlockDifficulty" => host(signatures::GET_BLOCK_DIFFICULTY, ids::GET_BLOCK_DIFFICULTY_FUNC),
			"getBlockGasLimit" => host(signatures::GET_BLOCK_GAS_LIMIT, ids::GET_BLOCK_GAS_LIMIT_FUNC),
			"getBlockHash" => host(signatures::GET_BLOCK_HASH, ids::GET_BLOCK_HASH_FUNC),
			"getBlockNumber" => host(signatures::GET_BLOCK_NUMBER, ids::GET_BLOCK_NUMBER_FUNC),
			"getBlockTimestamp" => host(signatures::GET_BLOCK_TIMESTAMP, ids::GET_BLOCK_TIMESTAMP_FUNC),
			"getTxGasPrice" => host(signatures::GET_TX_GAS_PRICE, ids::GET_TX_GAS_PRICE_FUNC),
			"getTxOrigin" => host(signatures::GET_TX_ORIGIN, ids::GET_TX_ORIGIN_FUNC),
			"log" => host(signatures::LOG, ids::LOG_FUNC),
			"call" => host(signatures::CALL, ids::CALL_FUNC),
			"callCode" => host(signatures::CALL_CODE, ids::CALL_CODE_FUNC),
			"callDelegate" => host(signatures::CALL_DELEGATE, ids::CALL_DELEGATE_FUNC),
			"callStatic" => host(signatures::CALL_STATIC, ids::CALL_STATIC_FUNC),
			"create" => host(signatures::CREATE, ids::CREATE_FUNC),
			"returnDataCopy" => host(signatures::RETURN_DATA_COPY, ids::RETURN_DATA_COPY_FUNC),
			"getReturnDataSize" => host(signatures::GET_RETURN_DATA_SIZE, ids::GET_RETURN_DATA_SIZE_FUNC),
			"finish" => host(signatures::FINISH, ids::FINISH_FUNC),
			"revert" => host(signatures::REVERT, ids::REVERT_FUNC),
			"callDataCopy" => host(signatures::CALL_DATA_COPY, ids::CALL_DATA_COPY_FUNC),
			"getCallDataSize" => host(signatures::GET_CALL_DATA_SIZE, ids::GET_CALL_DATA_SIZE_FUNC),
			"getCaller" => host(signatures::GET_CALLER, ids::GET_CALLER_FUNC),
			"getCallValue" => host(signatures::GET_CALL_VALUE, ids::GET_CALL_VALUE_FUNC),
			"codeCopy" => host(signatures::CODE_COPY, ids::CODE_COPY_FUNC),
			"getCodeSize" => host(signatures::GET_CODE_SIZE, ids::GET_CODE_SIZE_FUNC),
			"externalCodeCopy" => host(signatures::EXTERNAL_CODE_COPY, ids::EXTERNAL_CODE_COPY_FUNC),
			"getExternalCodeSize" => host(signatures::GET_EXTERNAL_CODE_SIZE, ids::GET_EXTERNAL_CODE_SIZE_FUNC),
			"storageLoad" => host(signatures::STORAGE_LOAD, ids::STORAGE_LOAD_FUNC),
			"storageStore" => host(signatures::STORAGE_STORE, ids::STORAGE_STORE_FUNC),
			"selfDestruct" => host(signatures::SELF_DESTRUCT, ids::SELF_DESTRUCT_FUNC),
			_ => {
				return Err(Error::Instantiation(
					format!("ETHEREUM export {} not found", field_name)))
			}
		};

		Ok(func_ref)
	}

	fn resolve_memory(
		&self,
		field_name: &str,
		descriptor: &MemoryDescriptor
	) -> std::result::Result<MemoryRef, Error> {
		if field_name == "memory" {
                        let mem = wasm_env::alloc_memory(descriptor, self.max_memory)?;
                        *self.memory.borrow_mut() = Some(mem.clone());
                        Ok(mem)
		} else {
			Err(Error::Instantiation("Memory imported under unknown name".to_owned()))
		}
	}
}
