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

// extern crate serde;
// extern crate serde_json;
// #[macro_use] extern crate serde_derive;
// extern crate ethereum_types;
// extern crate ethjson;

// extern crate ethcore_logger;
// extern crate rustc_hex;

// mod fixture;
// mod runner;

// use fixture::Fixture;
// use wasm::WasmInterpreter;
// use vm::{Exec, Ext, GasLeft, ActionParams, ActionValue, ParamsType};

// ################################################################################

extern crate log;
extern crate byteorder;
extern crate ethereum_types;
extern crate wasmi;
extern crate parity_wasm;
extern crate wasm;
extern crate vm;
extern crate clap;


use clap::{App, Arg};
use std::cell::RefCell;
use std::io::{self, Read, Cursor};
use std::{fs, path};

use log::trace;
use vm::tests::FakeExt;
use vm::CallType;
use ethereum_types::{U128, U256, H128, H256, Address};
use parity_wasm::elements::{self, Deserialize};
use wasmi::{Externals, RuntimeArgs, RuntimeValue,
	    MemoryRef, MemoryInstance, MemoryDescriptor,
	    FuncRef, FuncInstance, Signature,
	    Trap, TrapKind, Error as InterpreterError, memory_units};


fn load_code<P: AsRef<path::Path>>(p: P) -> io::Result<Vec<u8>> {
	let mut result = Vec::new();
	let mut f = fs::File::open(p)?;
	f.read_to_end(&mut result)?;
	Ok(result)
}


pub struct ImportResolver {
	memory: RefCell<Option<MemoryRef>>
}


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


pub mod signatures {

	use wasmi::{self, ValueType};
	use wasmi::ValueType::*;

	pub struct StaticSignature(pub &'static [ValueType], pub Option<ValueType>);

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

	impl Into<wasmi::Signature> for StaticSignature {
		fn into(self) -> wasmi::Signature {
			wasmi::Signature::new(self.0, self.1)
		}
	}

}


fn host(signature: signatures::StaticSignature, idx: usize) -> FuncRef {
	FuncInstance::alloc_host(signature.into(), idx)
}


impl wasmi::ModuleImportResolver for ImportResolver {

	fn resolve_func(
		&self,
		field_name: &str,
		_signature: &Signature
	) -> std::result::Result<FuncRef, InterpreterError> {

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
				return Err(InterpreterError::Instantiation(
					format!("ETHEREUM export {} not found", field_name)))
			}
		};

		Ok(func_ref)
	}

	fn resolve_memory(
		&self,
		field_name: &str,
		descriptor: &MemoryDescriptor
	) -> std::result::Result<MemoryRef, InterpreterError> {
		assert!(field_name == "memory");
		let init_mem = memory_units::Pages(descriptor.initial() as usize);
		let max_mem = descriptor.maximum().map(|x| memory_units::Pages(x as usize));
		let mem = MemoryInstance::alloc(init_mem, max_mem)?;
		*self.memory.borrow_mut() = Some(mem.clone());
		Ok(mem)
	}
}




/// User trap in native code
#[derive(Debug, Clone, PartialEq)]
pub enum Error {
	/// Storage read error
	StorageReadError,
	/// Storage update error
	StorageUpdateError,
	/// Memory access violation
	MemoryAccessViolation,
	/// Native code resulted in suicide
	Suicide,
	/// Native code requested execution to finish
	Return,
	/// Suicide was requested but coudn't complete
	SuicideAbort,
	/// Invalid gas state inside interpreter
	InvalidGasState,
	/// Query of the balance resulted in an error
	BalanceQueryError,
	/// Failed allocation
	AllocationFailed,
	/// Gas limit reached
	GasLimit,
	/// Unknown runtime function
	Unknown,
	/// Passed string had invalid utf-8 encoding
	BadUtf8,
	/// Log event error
	Log,
	/// Other error in native code
	Other,
	/// Syscall signature mismatch
	InvalidSyscall,
	/// Unreachable instruction encountered
	Unreachable,
	/// Invalid virtual call
	InvalidVirtualCall,
	/// Division by zero
	DivisionByZero,
	/// Invalid conversion to integer
	InvalidConversionToInt,
	/// Stack overflow
	StackOverflow,
	/// Panic with message
	Panic(String),
}

impl wasmi::HostError for Error { }

impl From<Trap> for Error {
	fn from(trap: Trap) -> Self {
		match *trap.kind() {
			TrapKind::Unreachable => Error::Unreachable,
			TrapKind::MemoryAccessOutOfBounds => Error::MemoryAccessViolation,
			TrapKind::TableAccessOutOfBounds | TrapKind::ElemUninitialized => Error::InvalidVirtualCall,
			TrapKind::DivisionByZero => Error::DivisionByZero,
			TrapKind::InvalidConversionToInt => Error::InvalidConversionToInt,
			TrapKind::UnexpectedSignature => Error::InvalidVirtualCall,
			TrapKind::StackOverflow => Error::StackOverflow,
			TrapKind::Host(_) => Error::Other,
		}
	}
}

impl From<InterpreterError> for Error {
	fn from(err: InterpreterError) -> Self {
		match err {
			InterpreterError::Value(_) => Error::InvalidSyscall,
			InterpreterError::Memory(_) => Error::MemoryAccessViolation,
			_ => Error::Other,
		}
	}
}


impl ::std::fmt::Display for Error {
	fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::result::Result<(), ::std::fmt::Error> {
		match *self {
			Error::StorageReadError => write!(f, "Storage read error"),
			Error::StorageUpdateError => write!(f, "Storage update error"),
			Error::MemoryAccessViolation => write!(f, "Memory access violation"),
			Error::SuicideAbort => write!(f, "Attempt to suicide resulted in an error"),
			Error::InvalidGasState => write!(f, "Invalid gas state"),
			Error::BalanceQueryError => write!(f, "Balance query resulted in an error"),
			Error::Suicide => write!(f, "Suicide result"),
			Error::Return => write!(f, "Return result"),
			Error::Unknown => write!(f, "Unknown runtime function invoked"),
			Error::AllocationFailed => write!(f, "Memory allocation failed (OOM)"),
			Error::BadUtf8 => write!(f, "String encoding is bad utf-8 sequence"),
			Error::GasLimit => write!(f, "Invocation resulted in gas limit violated"),
			Error::Log => write!(f, "Error occured while logging an event"),
			Error::InvalidSyscall => write!(f, "Invalid syscall signature encountered at runtime"),
			Error::Other => write!(f, "Other unspecified error"),
			Error::Unreachable => write!(f, "Unreachable instruction encountered"),
			Error::InvalidVirtualCall => write!(f, "Invalid virtual call"),
			Error::DivisionByZero => write!(f, "Division by zero"),
			Error::StackOverflow => write!(f, "Stack overflow"),
			Error::InvalidConversionToInt => write!(f, "Invalid conversion to integer"),
			Error::Panic(ref msg) => write!(f, "Panic: {}", msg),
		}
	}
}


pub struct RuntimeContext {
	pub address: Address,
	pub sender: Address,
	pub origin: Address,
	pub code_address: Address,
	pub value: U256,
}


pub struct Runtime<'a> {
	gas_counter: u64,
	gas_limit: u64,
	ext: &'a mut vm::Ext,
	context: RuntimeContext,
	memory: MemoryRef,
	args: Vec<u8>,
	result: Vec<u8>
}



mod panic_payload {

	use byteorder::{LittleEndian, ReadBytesExt};
	use std::io::{self, Read};

	#[derive(Debug, PartialEq, Eq)]
	pub struct PanicPayload {
		pub msg: Option<String>,
		pub file: Option<String>,
		pub line: Option<u32>,
		pub col: Option<u32>,
	}

	fn read_string(rdr: &mut io::Cursor<&[u8]>) -> io::Result<Option<String>> {
		let string_len = rdr.read_u32::<LittleEndian>()?;
		let string = if string_len == 0 {
			None
		} else {
			let mut content = vec![0; string_len as usize];
			rdr.read_exact(&mut content)?;
			Some(String::from_utf8_lossy(&content).into_owned())
		};
		Ok(string)
	}

	pub fn decode(raw: &[u8]) -> PanicPayload {
		let mut rdr = io::Cursor::new(raw);
		let msg = read_string(&mut rdr).ok().and_then(|x| x);
		let file = read_string(&mut rdr).ok().and_then(|x| x);
		let line = rdr.read_u32::<LittleEndian>().ok();
		let col = rdr.read_u32::<LittleEndian>().ok();
		PanicPayload {
			msg: msg,
			file: file,
			line: line,
			col: col,
		}
	}
}







type Result<T> = ::std::result::Result<T, Error>;

impl<'a> Runtime<'a> {

	/// Loads 256-bit hash from the specifed sandboxed memory pointer
	fn h256_at(&self, ptr: u32) -> Result<H256> {
		let mut buf = [0u8; 32];
		self.memory.get_into(ptr, &mut buf[..])?;

		Ok(H256::from(&buf[..]))
	}

	/// Loads 160-bit hash (Ethereum address) from the specified sandboxed memory pointer
	fn address_at(&self, ptr: u32) -> Result<Address> {
		let mut buf = [0u8; 20];
		self.memory.get_into(ptr, &mut buf[..])?;
		Ok(Address::from(&buf[..]))
	}

	/// Loads 128-bit integer represented with bigendian from the specified sandboxed memory pointer
	fn u128_at(&self, ptr: u32) -> Result<U128> {
		let mut buf = [0u8; 16];
		self.memory.get_into(ptr, &mut buf[..])?;
		Ok(U128::from_big_endian(&buf[..]))
	}

	/// Loads 256-bit integer represented with bigendian from the specified sandboxed memory pointer
	// fn u256_at(&self, ptr: u32) -> Result<U256> {
	// 	let mut buf = [0u8; 32];
	// 	self.memory.get_into(ptr, &mut buf[..])?;
	// 	Ok(U256::from_big_endian(&buf[..]))
	// }

	/// Charge specified amount of gas
	///
	/// Returns false if gas limit exceeded and true if not.
	/// Intuition about the return value sense is to aswer the question 'are we allowed to continue?'
	fn charge_gas(&mut self, amount: u64) -> bool {
		let prev = self.gas_counter;
		match prev.checked_add(amount) {
			// gas charge overflow protection
			None => false,
			Some(val) if val > self.gas_limit => false,
			Some(_) => {
				self.gas_counter = prev + amount;
				true
			}
		}
	}

	/// Charge gas according to closure
	pub fn charge<F>(&mut self, f: F) -> Result<()>
		where F: FnOnce(&vm::Schedule) -> u64
	{
		let amount = f(self.ext.schedule());
		if !self.charge_gas(amount as u64) {
			Err(Error::GasLimit)
		} else {
			Ok(())
		}
	}

	/// Adjusted charge of gas which scales actual charge according to the wasm opcode counting coefficient
	pub fn adjusted_charge<F>(&mut self, f: F) -> Result<()>
		where F: FnOnce(&vm::Schedule) -> u64
	{
		self.charge(|schedule| f(schedule) * schedule.wasm().opcodes_div as u64 / schedule.wasm().opcodes_mul as u64)
	}


	/// Charge gas provided by the closure
	///
	/// Closure also can return overflowing flag as None in gas cost.
	pub fn overflow_charge<F>(&mut self, f: F) -> Result<()>
		where F: FnOnce(&vm::Schedule) -> Option<u64>
	{
		let amount = match f(self.ext.schedule()) {
			Some(amount) => amount,
			None => { return Err(Error::GasLimit.into()); }
		};

		if !self.charge_gas(amount as u64) {
			Err(Error::GasLimit.into())
		} else {
			Ok(())
		}
	}

	/// Same as overflow_charge, but with amount adjusted by wasm opcodes coeff
	pub fn adjusted_overflow_charge<F>(&mut self, f: F) -> Result<()>
		where F: FnOnce(&vm::Schedule) -> Option<u64>
	{
		self.overflow_charge(|schedule|
			f(schedule)
				.and_then(|x| x.checked_mul(schedule.wasm().opcodes_div as u64))
				.map(|x| x / schedule.wasm().opcodes_mul as u64)
		)
	}


	/// Return currently used schedule
	pub fn schedule(&self) -> &vm::Schedule {
		self.ext.schedule()
	}

	pub fn gas_left(&self) -> Result<u64> {
		if self.gas_counter > self.gas_limit { return Err(Error::InvalidGasState); }
		Ok(self.gas_limit - self.gas_counter)
	}

	fn return_address_ptr(&mut self, ptr: u32, val: Address) -> Result<()> {
		self.charge(|schedule| schedule.wasm().static_address as u64)?;
		self.memory.set(ptr, &*val)?;
		Ok(())
	}

	fn return_u128_ptr(&mut self, ptr: u32, val: U128) -> Result<()> {
		let value: H128 = val.into();
		self.charge(|schedule| schedule.wasm().static_u256 as u64)?; // costs as U256
		self.memory.set(ptr, &*value)?;
		Ok(())
	}

	fn return_u256_ptr(&mut self, ptr: u32, val: U256) -> Result<()> {
		let value: H256 = val.into();
		self.charge(|schedule| schedule.wasm().static_u256 as u64)?;
		self.memory.set(ptr, &*value)?;
		Ok(())
	}


	fn do_create(
		&mut self,
		endowment: U256,
		code_ptr: u32,
		code_len: u32,
		result_ptr: u32,
		scheme: vm::CreateContractAddress
	) -> Result<RuntimeValue> {
		let code = self.memory.get(code_ptr, code_len as usize)?;

		self.adjusted_charge(|schedule| schedule.create_gas as u64)?;
		self.adjusted_charge(|schedule| schedule.create_data_gas as u64 * code.len() as u64)?;

		let gas_left: U256 = U256::from(self.gas_left()?)
			* U256::from(self.ext.schedule().wasm().opcodes_mul)
			/ U256::from(self.ext.schedule().wasm().opcodes_div);

		match self.ext.create(&gas_left, &endowment, &code, scheme, false).ok().expect("Trap is false; trap error will not happen; qed") {
			vm::ContractCreateResult::Created(address, gas_left) => {
				trace!(target: "wasm", "runtime: create contract success (@{:?})", address);
				self.result.resize(0, 0); // clear return buffer
				self.memory.set(result_ptr, &*address)?;
				self.gas_counter = self.gas_limit -
					// this cannot overflow, since initial gas is in [0..u64::max) range,
					// and gas_left cannot be bigger
					gas_left.low_u64() * self.ext.schedule().wasm().opcodes_div as u64
						/ self.ext.schedule().wasm().opcodes_mul as u64;
				Ok(0i32.into())
			},
			vm::ContractCreateResult::Failed => {
				trace!(target: "wasm", "runtime: create contract fail");
				Ok(1i32.into())
			},
			vm::ContractCreateResult::Reverted(gas_left, data) => {
				trace!(target: "wasm", "runtime: create contract reverted");
				self.result.resize(data.len(), 0);
				self.result.copy_from_slice(&data[..]);
				self.gas_counter = self.gas_limit -
					// this cannot overflow, since initial gas is in [0..u64::max) range,
					// and gas_left cannot be bigger
					gas_left.low_u64() * self.ext.schedule().wasm().opcodes_div as u64
						/ self.ext.schedule().wasm().opcodes_mul as u64;

				Ok(2i32.into())
			},
		}
	}


	fn do_call(
		&mut self,
		use_val: bool,
		call_type: CallType,
		args: RuntimeArgs,
	) -> Result<RuntimeValue> {
		trace!(target: "wasm", "runtime: CALL({:?})", call_type);

		let gas: u64 = args.nth_checked(0)?;
		trace!(target: "wasm", "           gas: {:?}", gas);

		let address = self.address_at(args.nth_checked(1)?)?;
		trace!(target: "wasm", "       address: {:?}", address);

		let (vofs, val) = if use_val {
			let val: U128 = self.u128_at(args.nth_checked(2)?)?;
			(1usize, Some(U256::from(val)))
		} else {
			(0usize, None)
		};


		trace!(target: "wasm", "           val: {:?}", val);

		let input_ptr: u32 = args.nth_checked(2 + vofs)?;
		trace!(target: "wasm", "     input_ptr: {:?}", input_ptr);

		let input_len: u32 = args.nth_checked(3 + vofs)?;
		trace!(target: "wasm", "     input_len: {:?}", input_len);

		if let Some(ref val) = val {
			let address_balance = self.ext.balance(&self.context.address)
				.map_err(|_| Error::BalanceQueryError)?;

			if &address_balance < val {
				trace!(target: "wasm", "runtime: call failed due to balance check");
				return Ok((-1i32).into());
			}
		}

		self.adjusted_charge(|schedule| schedule.call_gas as u64)?;

		// todo: optimize to use memory views once it's in
		let payload = self.memory.get(input_ptr, input_len as usize)?;

		let adjusted_gas = match gas.checked_mul(self.ext.schedule().wasm().opcodes_div as u64)
			.map(|x| x / self.ext.schedule().wasm().opcodes_mul as u64)
		{
			Some(x) => x,
			None => {
				trace!("CALL overflowed gas, call aborted with error returned");
				return Ok(RuntimeValue::I32(-1))
			},
		};

		self.charge(|_| adjusted_gas)?;

		let call_result = self.ext.call(
			&gas.into(),
			match call_type {
				CallType::DelegateCall => &self.context.sender,
				_ => &self.context.address
			},
			match call_type {
				CallType::Call | CallType::StaticCall => &address,
				_ => &self.context.address
			},
			val,
			&payload,
			&address,
			call_type,
			false
		).ok().expect("Trap is false; trap error will not happen; qed");

		match call_result {
			vm::MessageCallResult::Success(gas_left, data) => {
				let mut result = Vec::with_capacity(data.len());
				result.copy_from_slice(&data[..]);
				self.result = result;

				// cannot overflow, before making call gas_counter was incremented with gas, and gas_left < gas
				self.gas_counter = self.gas_counter -
					gas_left.low_u64() * self.ext.schedule().wasm().opcodes_div as u64
					/ self.ext.schedule().wasm().opcodes_mul as u64;
				Ok(0i32.into())
			},
			vm::MessageCallResult::Failed  => {
				Ok(1i32.into())
			}
			vm::MessageCallResult::Reverted(gas_left, data) => {
				let mut result = Vec::with_capacity(data.len());
				result.copy_from_slice(&data[..]);
				self.result = result;

				// cannot overflow, before making call gas_counter was incremented with gas, and gas_left < gas
				self.gas_counter = self.gas_counter -
					gas_left.low_u64() * self.ext.schedule().wasm().opcodes_div as u64
					/ self.ext.schedule().wasm().opcodes_mul as u64;
				Ok(2i32.into())
			},
		}
	}



	// ################################################################################
	// EWASM specific callbacks - essentially PWASM with different names where possible

	/// Returns the current gasCounter
	///
	/// Parameters: nothing
	///
	/// Returns: gasLeft i64
	pub fn get_gas_left(&self) -> Result<RuntimeValue> {
		Ok(RuntimeValue::I64(self.gas_left()? as i64))
	}

	/// Gets address of currently executing account and stores it in memory at the given offset.
	///
	/// Parameters:
	/// resultOffset i32ptr - the memory offset at which the address is to be stored (address)
	///
	/// Returns: nothing
	///
	/// Trap conditions:
	/// store to memory at resultOffset results in out of bounds access.
	pub fn get_address(&mut self, args: RuntimeArgs) -> Result<()> {
		let address = self.context.address;
		self.return_address_ptr(args.nth_checked(0)?, address)
	}

	/// Gets the block’s beneficiary address and loads into memory.
	///
	/// Parameters:
	/// resultOffset i32ptr - the memory offset to load the coinbase address into (address)
	///
	/// Returns: nothing
	///
	/// Trap conditions:
	/// store address to memory at resultOffset results in out of bounds access.
	pub fn get_block_coinbase(&mut self, args: RuntimeArgs) -> Result<()> {
		let coinbase = self.ext.env_info().author;
		self.return_address_ptr(args.nth_checked(0)?, coinbase)
	}


	/// Get the block’s difficulty.
	///
	/// Parameters:
	/// resultOffset i32ptr the memory offset to load the difficulty into (u256)
	///
	/// Returns: nothing
	///
	/// Trap conditions:
	/// store u256 to memory at resultOffset results in out of bounds access.
	pub fn get_block_difficulty(&mut self, args: RuntimeArgs) -> Result<()> {
		let difficulty = self.ext.env_info().difficulty;
		self.return_u256_ptr(args.nth_checked(0)?, difficulty)
	}

	/// Get the block’s gas limit.
	///
	/// Parameters: nothing
	///
	/// Returns:
	/// blockGasLimit i64
	pub fn get_block_gas_limit(&mut self) -> Result<RuntimeValue> {
		let gas_limit = self.ext.env_info().gas_limit;
		if gas_limit > U256::from(std::u64::MAX) {
			return Err(Error::Other); // not great, huh?
		}
		Ok(RuntimeValue::I64(gas_limit.low_u64() as i64))
	}

	/// Gets the hash of one of the 256 most recent complete blocks.
	///
	/// Parameters:
	/// number i64 - which block to load
	/// resultOffset i32ptr - the memory offset to load the hash into (bytes32)
	///
	/// Returns: result i32 - 0 on success and 1 on failure
	///
	/// Note: in case of failure, the output memory pointed by resultOffset is unchanged.
	///
	/// Trap conditions:
	/// store to memory at resultOffset results in out of bounds access (also checked on failure).
	pub fn get_block_hash(&mut self, args: RuntimeArgs) -> Result<RuntimeValue> {
		self.adjusted_charge(|schedule| schedule.blockhash_gas as u64)?;
		if let Ok(block_num) = args.nth_checked::<u64>(0) {
			let hash = self.ext.blockhash(&U256::from(block_num));
			self.memory.set(args.nth_checked(1)?, &*hash)?;
			return Ok(RuntimeValue::I32(0))
		}
		Ok(RuntimeValue::I32(1))
	}

	/// Get the block’s number.
	///
	/// Parameters: nothing
	///
	/// Returns: blockNumber i64
	pub fn get_block_number(&mut self) -> Result<RuntimeValue> {
		Ok(RuntimeValue::I64(self.ext.env_info().number as i64))
	}

	/// Get the block’s timestamp.
	///
	/// Parameters: nothing
	///
	/// Returns: blockTimestamp i64
	pub fn get_block_timestamp(&mut self) -> Result<RuntimeValue> {
		let timestamp = self.ext.env_info().timestamp;
		Ok(RuntimeValue::I64(timestamp as i64))
	}

	/// Gets the execution's origination address and loads it into memory at the given offset.
	/// This is the sender of original transaction; it is never an account with non-empty associated code.
	///
	/// Parameters:
	/// resultOffset i32ptr - the memory offset to load the origin address from (address)
	///
	/// Returns: nothing
	///
	/// Trap conditions:
	/// store address to memory at resultOffset results in out of bounds access.
	pub fn get_tx_origin(&mut self, args: RuntimeArgs) -> Result<()> {
		let origin = self.context.origin;
		self.return_address_ptr(args.nth_checked(0)?, origin)
	}

	/// Gets caller address and loads it into memory at the given offset.
	/// This is the address of the account that is directly responsible for this execution.
	///
	/// Parameters:
	/// resultOffset i32ptr - the memory offset to load the address into (address)
	///
	/// Returns: nothing
	///
	/// Trap conditions:
	/// store address to memory at resultOffset results in out of bounds access.
	pub fn get_caller(&mut self, args: RuntimeArgs) -> Result<()> {
		let sender = self.context.sender;
		self.return_address_ptr(args.nth_checked(0)?, sender)
	}


	/// Copies the input data in current environment to memory.
	/// This pertains to the input data passed with the message call instruction or transaction.
	///
	/// Parameters:
	/// resultOffset i32ptr - the memory offset to load data into (bytes)
	/// dataOffset i32 - the offset in the input data
	/// length i32 - the length of data to copy
	///
	/// Returns: nothing
	///
	/// Trap conditions:
	/// load length number of bytes from input data buffer at dataOffset results in out of bounds access,
	/// store length number of bytes to memory at resultOffset results in out of bounds access.
	pub fn call_data_copy(&mut self, args: RuntimeArgs) -> Result<()> {
		let ptr: u32 = args.nth_checked(0)?;
		let offset: u32 = args.nth_checked(1)?;
		let length: u32 = args.nth_checked(2)?;

		let args_len = self.args.len() as u64;
		if offset + length > args_len as u32 - offset {
			return Err(Error::MemoryAccessViolation)
		}

		self.charge(|s| args_len * s.wasm().memcpy as u64)?;

		self.memory.set(ptr, &self.args[offset as usize..(offset + length) as usize])?;
		Ok(())
	}

	/// Gets the deposited value by the instruction/transaction responsible for this execution and loads it into memory at the given location.
	///
	/// Parameters:
	/// resultOffset i32ptr - the memory offset to load the value into (u128)
	///
	/// Returns: nothing
	///
	/// Trap conditions:
	/// store u128 to memory at resultOffset results in out of bounds access.
	pub fn get_call_value(&mut self, args: RuntimeArgs) -> Result<()> {
		let val: U256 = self.context.value;
		if val > U256::from(U128::MAX) {
			return Err(Error::Panic("call value larger than U128::MAX".into()))
		}
		self.return_u128_ptr(args.nth_checked(0)?, val.into())?;
		Ok(())
	}

	/// Get size of input data in current environment.
	/// This pertains to the input data passed with the message call instruction or transaction.
	///
	/// Parameters: none
	///
	/// Returns:
	/// callDataSize i32
	pub fn get_call_data_size(&mut self) -> RuntimeValue {
		RuntimeValue::I32(self.args.len() as i32)
	}


	/// Creates a new log in the current environment
	///
	/// Parameters:
	/// dataOffset i32ptr - the memory offset to load data from (bytes)
	/// dataLength i32 - the data length
	/// numberOfTopics i32 - the number of topics following (0 to 4)
	/// topic1 i32ptr - the memory offset to load topic1 from (bytes32)
	/// topic2 i32ptr - the memory offset to load topic2 from (bytes32)
	/// topic3 i32ptr - the memory offset to load topic3 from (bytes32)
	/// topic4 i32ptr - the memory offset to load topic4 from (bytes32)
	///
	/// Returns: nothing
	///
	/// Trap conditions:
	/// load dataLength number of bytes from memory at dataOffset results in out of bounds access,
	/// numberOfTopics is greater than 4,
	/// load bytes32 from memory at topic1 results in out of bounds access,
	/// load bytes32 from memory at topic2 results in out of bounds access,
	/// load bytes32 from memory at topic3 results in out of bounds access,
	/// load bytes32 from memory at topic4 results in out of bounds access.
	pub fn log(&mut self, args: RuntimeArgs) -> Result<()>
	{
		let data_ptr: u32 = args.nth_checked(0)?;
		let data_len: u32 = args.nth_checked(1)?;
		let topic_count: u32 = args.nth_checked(2)?;

		if topic_count > 4 {
			return Err(Error::Log.into());
		}

		let mut topics: Vec<H256> = Vec::with_capacity(topic_count as usize);
		for arg_i in 0..=topic_count {
			let topic_ptr: u32 = args.nth_checked(3 + arg_i as usize)?;
			let topic: H256 = H256::from(&self.memory.get(topic_ptr, 32)?[..]);
			topics.push(topic);
		}

		self.adjusted_overflow_charge(|schedule| {
			let topics_gas =
				schedule.log_gas as u64 +
				schedule.log_topic_gas as u64 * topic_count as u64;
			(schedule.log_data_gas as u64)
				.checked_mul(schedule.log_data_gas as u64)
				.and_then(|data_gas| data_gas.checked_add(topics_gas))
		})?;

		self.ext.log(topics, &self.memory.get(data_ptr, data_len as usize)?)
			.map_err(|_| Error::Log)?;

		Ok(())
	}




	/// Sends a message with arbitrary data to a given address path
	///
	/// Parameters:
	/// gas i64 - the gas limit
	/// addressOffset i32ptr - the memory offset to load the address from (address)
	/// valueOffset i32ptr - the memory offset to load the value from (u128)
	/// dataOffset i32ptr - the memory offset to load data from (bytes)
	/// dataLength i32 - the length of data
	///
	/// Returns: result i32 Returns 0 on success, 1 on failure and 2 on revert
	///
	/// Trap conditions:
	/// load address from memory at addressOffset results in out of bounds access,
	/// load u128 from memory at valueOffset results in out of bounds access,
	/// load dataLength number of bytes from memory at dataOffset results in out of bounds access.
	pub fn call(&mut self, args: RuntimeArgs) -> Result<RuntimeValue> {
		self.do_call(true, CallType::Call, args)
	}

	/// Message-call into this account with an alternative account's code.
	///
	/// Parameters:
	/// gas i64 - the gas limit
	/// addressOffset i32ptr - the memory offset to load the address from (address)
	/// valueOffset i32ptr - the memory offset to load the value from (u128)
	/// dataOffset i32ptr - the memory offset to load data from (bytes)
	/// dataLength i32 - the length of data
	///
	/// Returns: result i32 Returns 0 on success, 1 on failure and 2 on revert
	///
	/// Trap conditions:
	/// load address from memory at addressOffset results in out of bounds access,
	/// load u128 from memory at valueOffset results in out of bounds access,
	/// load dataLength number of bytes from memory at dataOffset results in out of bounds access.
	pub fn call_code(&mut self, args: RuntimeArgs) -> Result<RuntimeValue> {
		self.do_call(true, CallType::CallCode, args)
	}

	/// Message-call into this account with an alternative account’s code, but persisting the current values for sender and value.
	///
	/// Parameters:
	/// gas i64 - the gas limit
	/// addressOffset i32ptr - the memory offset to load the address from (address)
	/// dataOffset i32ptr - the memory offset to load data from (bytes)
	/// dataLength i32 - the length of data
	///
	/// Returns: result i32 Returns 0 on success, 1 on failure and 2 on revert
	///
	/// Trap conditions:
	/// load address from memory at addressOffset results in out of bounds access,
	/// load dataLength number of bytes from memory at dataOffset results in out of bounds access.
	pub fn call_delegate(&mut self, args: RuntimeArgs) -> Result<RuntimeValue> {
		self.do_call(false, CallType::DelegateCall, args)
	}

	/// Sends a message with arbitrary data to a given address path, but disallow state modifications. This includes log, create, selfdestruct and call with a non-zero value.
	///
	/// Parameters:
	/// gas i64 - the gas limit
	/// addressOffset i32ptr - the memory offset to load the address from (address)
	/// dataOffset i32ptr - the memory offset to load data from (bytes)
	/// dataLength i32 - the length of data
	///
	/// Returns - result i32 Returns 0 on success, 1 on failure and 2 on revert
	///
	/// Trap conditions:
	/// load address from memory at addressOffset results in out of bounds access,
	/// load dataLength number of bytes from memory at dataOffset results in out of bounds access.
	pub fn call_static(&mut self, args: RuntimeArgs) -> Result<RuntimeValue> {
		self.do_call(false, CallType::StaticCall, args)
	}

	/// Creates a new contract with a given value.
	///
	/// Parameters:
	/// valueOffset i32ptr - the memory offset to load the value from (u128)
	/// dataOffset i32ptr - the memory offset to load the code for the new contract from (bytes)
	/// dataLength i32 - the data length
	/// resultOffset i32ptr - the memory offset to write the new contract address to (address)
	///
	/// Note: create will clear the return buffer in case of success or may fill it with data coming from revert.
	///
	/// Returns: result i32 - 0 on success, 1 on failure and 2 on revert
	///
	/// Trap conditions:
	/// load u128 from memory at valueOffset results in out of bounds access,
	/// load dataLength number of bytes from memory at dataOffset results in out of bounds access.
	/// store address to memory at resultOffset results in out of bounds access.
	pub fn create(&mut self, args: RuntimeArgs) -> Result<RuntimeValue> {
		trace!(target: "wasm", "runtime: CREATE");
		let endowment = self.u128_at(args.nth_checked(0)?)?;
		trace!(target: "wasm", "       val: {:?}", endowment);
		let code_ptr: u32 = args.nth_checked(1)?;
		trace!(target: "wasm", "  code_ptr: {:?}", code_ptr);
		let code_len: u32 = args.nth_checked(2)?;
		trace!(target: "wasm", "  code_len: {:?}", code_len);
		let result_ptr: u32 = args.nth_checked(3)?;
		trace!(target: "wasm", "result_ptr: {:?}", result_ptr);
		self.do_create(U256::from(endowment), code_ptr, code_len, result_ptr,
			       vm::CreateContractAddress::FromSenderAndCodeHash)
	}




	/// Copies the current return data buffer to memory.
	/// This contains the return data from last executed call, callCode, callDelegate, callStatic or create.
	///
	/// Note: create only fills the return data buffer in case of a failure.
	///
	/// Parameters:
	/// resultOffset i32ptr - the memory offset to load data into (bytes)
	/// dataOffset i32 - the offset in the return data
	/// length i32 - the length of data to copy
	///
	/// Returns: nothing
	///
	/// Trap conditions:
	/// load length number of bytes from input data buffer at dataOffset results in out of bounds access,
	/// store length number of bytes to memory at resultOffset results in out of bounds access.
	pub fn return_data_copy(&mut self, args: RuntimeArgs) -> Result<()> {
		let ptr: u32 = args.nth_checked(0)?;
		let offset: u32 = args.nth_checked(1)?;
		let length: u32 = args.nth_checked(2)?;

		if offset + length > self.result.len() as u32 {
			return Err(Error::MemoryAccessViolation);
		}

		// TODO: charge some gas here?

		self.memory.set(ptr, &self.result[offset as usize..(offset + length) as usize])?;

		Ok(())
	}

	/// Get size of current return data buffer to memory.
	/// This contains the return data from the last executed call, callCode, callDelegate, callStatic or create.
	///
	/// Note: create only fills the return data buffer in case of a failure.
	///
	/// Parameters: none
	///
	/// Returns: dataSize i32
	pub fn get_return_data_size(&mut self) -> RuntimeValue {
		RuntimeValue::I32(self.result.len() as i32)
	}

	/// Set the returning output data for the execution.
	/// This will cause a trap and the execution will be aborted immediately.
	///
	/// Parameters:
	/// dataOffset i32ptr - the memory offset of the output data (bytes)
	/// dataLength i32 - the length of the output data
	///
	/// Returns: doesn't return
	///
	/// Trap conditions:
	/// load dataLength number of bytes from memory at dataOffset results in out of bounds access.
	pub fn finish(&mut self, args: RuntimeArgs) -> Result<()> {
		let ptr: u32 = args.nth_checked(0)?;
		let len: u32 = args.nth_checked(1)?;

		trace!(target: "wasm", "Contract ret: {} bytes @ {}", len, ptr);

		self.result = self.memory.get(ptr, len as usize)?;

		Err(Error::Return)
	}

	/// Set the returning output data for the execution.
	/// This will cause a trap and the execution will be aborted immediately.
	///
	/// Parameters:
	/// dataOffset i32ptr - the memory offset of the output data (bytes)
	/// dataLength i32 - the length of the output data
	///
	/// Returns: doesn't return
	///
	/// Trap conditions:
	/// load dataLength number of bytes from memory at dataOffset results in out of bounds access.
	pub fn revert(&mut self, args: RuntimeArgs) -> Result<()> {
		let payload_ptr: u32 = args.nth_checked(0)?;
		let payload_len: u32 = args.nth_checked(1)?;

		let raw_payload = self.memory.get(payload_ptr, payload_len as usize)?;
		let payload = panic_payload::decode(&raw_payload);
		let msg = format!(
			"{msg}, {file}:{line}:{col}",
			msg = payload
				.msg
				.as_ref()
				.map(String::as_ref)
				.unwrap_or("<msg was stripped>"),
			file = payload
				.file
				.as_ref()
				.map(String::as_ref)
				.unwrap_or("<unknown>"),
			line = payload.line.unwrap_or(0),
			col = payload.col.unwrap_or(0)
		);
		trace!(target: "wasm", "Contract custom panic message: {}", msg);

		Err(Error::Panic(msg).into())
	}

	/// Loads a 256-bit a value to memory from persistent storage
	///
	/// Parameters:
	/// pathOffset i32ptr - the memory offset to load the path from (bytes32)
	/// resultOffset i32ptr - the memory offset to store the result at (bytes32)
	///
	/// Returns: nothing
	///
	/// Trap conditions:
	/// load bytes32 from memory at pathOffset results in out of bounds access,
	/// store bytes32 to memory at resultOffset results in out of bounds access.
	pub fn storage_load(&mut self, args: RuntimeArgs) -> Result<()> {
		let key = self.h256_at(args.nth_checked(0)?)?;
		let val_ptr: u32 = args.nth_checked(1)?;

		let val = self.ext.storage_at(&key).map_err(|_| Error::StorageReadError)?;

		self.adjusted_charge(|schedule| schedule.sload_gas as u64)?;

		self.memory.set(val_ptr as u32, &*val)?;

		Ok(())
	}

	/// Store 256-bit a value in memory to persistent storage
	///
	/// Parameters:
	/// pathOffset i32ptr - the memory offset to load the path from (bytes32)
	/// valueOffset i32ptr - the memory offset to load the value from (bytes32)
	///
	/// Returns: nothing
	///
	/// Trap conditions:
	/// load bytes32 from memory at pathOffset results in out of bounds access,
	/// load bytes32 from memory at valueOffset results in out of bounds access.
	pub fn storage_store(&mut self, args: RuntimeArgs) -> Result<()> {
		let key = self.h256_at(args.nth_checked(0)?)?;
		let val_ptr: u32 = args.nth_checked(1)?;

		let val = self.h256_at(val_ptr)?;
		let former_val = self.ext.storage_at(&key).map_err(|_| Error::StorageUpdateError)?;

		if former_val == H256::zero() && val != H256::zero() {
			self.adjusted_charge(|schedule| schedule.sstore_set_gas as u64)?;
		} else {
			self.adjusted_charge(|schedule| schedule.sstore_reset_gas as u64)?;
		}

		self.ext.set_storage(key, val).map_err(|_| Error::StorageUpdateError)?;

		if former_val != H256::zero() && val == H256::zero() {
			let sstore_clears_schedule = self.schedule().sstore_refund_gas;
			self.ext.add_sstore_refund(sstore_clears_schedule);
		}

		Ok(())
	}

	/// Mark account for later deletion and give the remaining balance to the specified beneficiary address.
	/// This will cause a trap and the execution will be aborted immediately.
	///
	/// Parameters:
	/// addressOffset i32ptr - the memory offset to load the address from (address)
	///
	/// Returns: doesn't return
	///
	/// Trap conditions:
	/// load address from memory at addressOffset results in out of bounds access.
	pub fn self_destruct(&mut self, args: RuntimeArgs) -> Result<()> {
		let refund_address = self.address_at(args.nth_checked(0)?)?;

		if self.ext.exists(&refund_address).map_err(|_| Error::SuicideAbort)? {
			trace!(target: "wasm", "Suicide: refund to existing address {}", refund_address);
			self.adjusted_charge(|schedule| schedule.suicide_gas as u64)?;
		} else {
			trace!(target: "wasm", "Suicide: refund to new address {}", refund_address);
			self.adjusted_charge(|schedule| schedule.suicide_to_new_account_cost as u64)?;
		}

		self.ext.suicide(&refund_address).map_err(|_| Error::SuicideAbort)?;

		// We send trap to interpreter so it should abort further execution
		Err(Error::Suicide.into())
	}

}



macro_rules! void {
	{ $e: expr } => { { $e?; Ok(None) } }
}

macro_rules! some {
	{ $e: expr } => { { Ok(Some($e?)) } }
}

macro_rules! cast {
	{ $e: expr } => { { Ok(Some($e)) } }
}

impl<'a> Externals for Runtime<'a> {

	fn invoke_index(
		&mut self,
		index: usize,
		args: RuntimeArgs
	) -> std::result::Result<Option<RuntimeValue>, Trap> {

		use ids::*;

		match index {
			// USE_GAS_FUNC => void!(self.use_gas(args)),
			GET_GAS_LEFT_FUNC => some!(self.get_gas_left()),
			GET_ADDRESS_FUNC => void!(self.get_address(args)),
			// GET_EXTERNAL_BALANCE_FUNC => void!(self.get_external_balance(args)),
			GET_BLOCK_COINBASE_FUNC => void!(self.get_block_coinbase(args)),
			GET_BLOCK_DIFFICULTY_FUNC => void!(self.get_block_difficulty(args)),
			GET_BLOCK_GAS_LIMIT_FUNC => some!(self.get_block_gas_limit()),
			GET_BLOCK_HASH_FUNC => some!(self.get_block_hash(args)),
			GET_BLOCK_NUMBER_FUNC => some!(self.get_block_number()),
			GET_BLOCK_TIMESTAMP_FUNC => some!(self.get_block_timestamp()),
			// GET_TX_GAS_PRICE_FUNC => void!(self.get_tx_gas_price(args)),
			GET_TX_ORIGIN_FUNC => void!(self.get_tx_origin(args)),
			LOG_FUNC => void!(self.log(args)),
			CALL_FUNC => some!(self.call(args)),
			CALL_CODE_FUNC => some!(self.call_code(args)),
			CALL_DELEGATE_FUNC => some!(self.call_delegate(args)),
			CALL_STATIC_FUNC => some!(self.call_static(args)),
			CREATE_FUNC => some!(self.create(args)),
			RETURN_DATA_COPY_FUNC => void!(self.return_data_copy(args)),
			GET_RETURN_DATA_SIZE_FUNC => cast!(self.get_return_data_size()),
			FINISH_FUNC => void!(self.finish(args)),
			REVERT_FUNC => void!(self.revert(args)),
			CALL_DATA_COPY_FUNC => void!(self.call_data_copy(args)),
			GET_CALL_DATA_SIZE_FUNC => cast!(self.get_call_data_size()),
			GET_CALLER_FUNC => void!(self.get_caller(args)),
			GET_CALL_VALUE_FUNC => void!(self.get_call_value(args)),
			// CODE_COPY_FUNC => void!(self.code_copy(args)),
			// GET_CODE_SIZE_FUNC => cast!(self.get_code_size(args)),
			// EXTERNAL_CODE_COPY_FUNC => void!(self.external_code_copy(args)),
			// GET_EXTERNAL_CODE_SIZE_FUNC => cast!(self.get_external_code_size(args)),
			STORAGE_LOAD_FUNC => void!(self.storage_load(args)),
			STORAGE_STORE_FUNC => void!(self.storage_store(args)),
			SELF_DESTRUCT_FUNC => void!(self.self_destruct(args)),
			_ => panic!("ethereum module doesn't provide function at index {}", index)
		}
	}
}




fn run(code: &Vec<u8>) -> Option<RuntimeValue> {

	let mut ext = FakeExt::new();

	let plain_module = elements::Module::deserialize(&mut Cursor::new(&code[..])).unwrap();

	// assert!(!plain_module.memory_section().map_or(false, |ms| ms.entries().len() > 0));

	let module = wasmi::Module::from_parity_wasm_module(plain_module).unwrap();

	let memory = MemoryInstance::alloc(memory_units::Pages(0),
					   Some(memory_units::Pages(0))).unwrap();

	let instantiation_resolver = ImportResolver {
		memory: RefCell::new(Some(memory))
	};

	let mut runtime = Runtime {
		gas_counter: 0,
		gas_limit: 1000000,
		ext: &mut ext,
		context: RuntimeContext {
			address: Address::default(),
			sender: Address::default(),
			origin: Address::default(),
			code_address: Address::default(),
			value: U256::zero()
		},
		memory: instantiation_resolver.memory.borrow().clone().unwrap(),
		args: Vec::new(),
		result: Vec::new()
	};

	let imports_builder = wasmi::ImportsBuilder::new()
		.with_resolver("env", &instantiation_resolver)
		.with_resolver("ethereum", &instantiation_resolver);

	let module_instance = wasmi::ModuleInstance::new(&module, &imports_builder).unwrap();

	let module_instance = module_instance.run_start(&mut runtime).unwrap();

	module_instance.invoke_export("main", &[], &mut runtime).unwrap()

}


fn main() {
	let matches = App::new("ewasm-run")
		.arg(Arg::with_name("target")
		     .index(1)
		     .required(true)
		     //.multiple(true)
		     .help("wasm binary"))
		.get_matches();

	let wasm_path = matches.value_of("target").expect("target wasm binary");

	match load_code(wasm_path) {
		Ok(code) => {
			println!("!!!!!!!!!! RES = {:?}", run(&code));
		},
		err => {
			println!("!!!!!!!!!! NOOOO = {:?}", err);
		}
	}
}



	// let n1: [u8; 32] = [1,2,3,4,5,6,7,8,
	// 		    9,1,2,3,4,5,6,7,
	// 		    8,9,1,2,3,4,5,6,
	// 		    7,8,9,1,2,3,4,5];

	// println!("////////// {:?}", U256::from_big_endian(&n1));

	// let x: U256 = U256::from_dec_str("455867356320691211509079568257800983799433843217927484866578299507062277125").unwrap();

	// let mut n2: [u8; 32] = [0; 32];

	// x.to_big_endian(&mut n2[..]);

	// //println!("////////// {:?}", (0..31).map(|i| n.byte(i)).collect::<Vec<u8>>());
	// println!("////////// {:?}", n2);


#[test]
fn xxx() -> () {

	let mut v = Vec::new();

	v.resize(100, 0);

	println!("////////// y len = {:?} = {:?}", v.len(), v);


}
