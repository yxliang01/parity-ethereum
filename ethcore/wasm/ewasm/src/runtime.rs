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

use log::trace;
use ethereum_types::{U128, U256, H128, H256, Address};
use vm::{self, CallType};
use wasmi::{MemoryRef, RuntimeArgs, RuntimeValue};
use wasm_exec_common::{panic_payload, runtime::{Result, Error, Context}};


pub struct Runtime<'a> {
	gas_counter: u64,
	gas_limit: u64,
	ext: &'a mut vm::Ext,
	context: Context,
	memory: MemoryRef,
	args: Vec<u8>,
	result: Vec<u8>
}

impl<'a> Runtime<'a> {

	/// New runtime for wasm contract with specified params
	pub fn with_params(
		ext: &mut vm::Ext,
		memory: MemoryRef,
		gas_limit: u64,
		args: Vec<u8>,
		context: Context,
	) -> Runtime {
		Runtime {
			gas_counter: 0,
			gas_limit: gas_limit,
			memory: memory,
			ext: ext,
			context: context,
			args: args,
			result: Vec::new(),
		}
	}

	/// Destroy the runtime, returning currently recorded result of the execution
	pub fn into_result(self) -> Vec<u8> {
		self.result
	}

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

	/// Subtracts an amount to the gas counter
	///
	/// Parameters:
	/// amount i64 - the amount to subtract to the gas counter
	///
	/// Returns: nothing
	pub fn use_gas(&mut self, args: RuntimeArgs) -> Result<()> {
		let amount: u64 = args.nth_checked(0)?;
		let prev = self.gas_counter;
		match prev.checked_add(amount) {
			Some(val) if val <= self.gas_limit =>
				self.gas_counter = val,
			_ =>
			// not great since EWASM's use_gas fn doesn't signal to the caller
			// that gas limit is exceeded, VM finds this afterward
				self.gas_counter = std::u64::MAX
		}
		Ok(())
	}

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

	/// Gets balance of the given account and loads it into memory at the given offset.
	///
	/// Parameters:
	/// addressOffset i32ptr - the memory offset to load the address from (address)
	/// resultOffset i32ptr - the memory offset to load the balance into (u128)
	///
	/// Returns: nothing
	///
	/// Trap conditions:
	/// load from memory at addressOffset results in out of bounds access,
	/// store to memory at resultOffset results in out of bounds access.
	pub fn get_external_balance(&mut self, args: RuntimeArgs) -> Result<()> {
		let address = self.address_at(args.nth_checked(0)?)?;
		let result_ptr: u32 = args.nth_checked(1)?;

		if let Ok(balance) = self.ext.balance(&address) {
			if balance > U128::MAX.into() {
				return Err(Error::Other); // not great, huh?
			}
			self.return_u128_ptr(result_ptr, U128::from(balance))?;
			Ok(())
		} else {
			Err(Error::BalanceQueryError)
		}
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

	/// Gets price of gas in current environment.
	///
	/// Parameters:
	/// resultOffset i32ptr - the memory offset to write the value to (u128)
	///
	/// Returns: nothing
	///
	/// Trap conditions:
	/// store u128 to memory at resultOffset results in out of bounds access.
	pub fn get_tx_gas_price(&mut self, args: RuntimeArgs) -> Result<()> {
		let ptr: u32 = args.nth_checked(0)?;
		let price = U128::from(self.schedule().tx_gas);
		self.return_u128_ptr(ptr, price)
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

	/// Gets the size of code running in current environment.
	///
	/// Parameters: none
	///
	/// Returns: codeSize i32
	pub fn get_code_size(&mut self) -> Result<RuntimeValue> {
		let address = self.context.code_address;

		if let Ok(Some(size)) = self.ext.extcodesize(&address) {
			Ok(RuntimeValue::I32(size as i32))
		} else {
			let msg = format!("couldn't determine code size at address {}", address);
			Err(Error::Panic(msg).into())
		}
	}

	/// Copies the code running in current environment to memory.
	///
	/// Parameters:
	/// resultOffset i32ptr - the memory offset to load the result into (bytes)
	/// codeOffset i32 - the offset within the code
	/// length i32 - the length of code to copy
	///
	/// Returns: nothing
	///
	/// Trap conditions:
	/// load length number of bytes from the current code buffer at codeOffset results in out of bounds access,
	/// store length number of bytes to memory at resultOffset results in out of bounds access.
	pub fn code_copy(&mut self, args: RuntimeArgs) -> Result<()> {
		let address = self.context.code_address;
		let result_ptr: u32 = args.nth_checked(0)?;
		let code_ptr: u32 = args.nth_checked(1)?;
		let code_len: u32 = args.nth_checked(2)?;

		if let Ok(Some(code)) = self.ext.extcode(&address) {
			let code_range = code_ptr as usize..(code_ptr + code_len) as usize;
			self.memory.set(result_ptr, &code[code_range])?;
			Ok(())
		} else {
			let msg = format!("couldn't fetch code at address {}", address);
			Err(Error::Panic(msg).into())
		}
	}

	/// Get size of an account’s code.
	///
	/// Parameters:
	/// addressOffset - i32ptr the memory offset to load the address from (address)
	///
	/// Returns: extCodeSize i32
	///
	/// Trap conditions:
	/// load address from memory at addressOffset results in out of bounds access.
	pub fn get_external_code_size(&mut self, args: RuntimeArgs) -> Result<RuntimeValue> {
		let address = self.address_at(args.nth_checked(0)?)?;

		if let Ok(Some(size)) = self.ext.extcodesize(&address) {
			Ok(RuntimeValue::I32(size as i32))
		} else {
			let msg = format!("couldn't determine code size at address {}", address);
			Err(Error::Panic(msg).into())
		}
	}

	/// Copies the code of an account to memory.
	///
	/// Parameters:
	/// addressOffset i32ptr - the memory offset to load the address from (address)
	/// resultOffset i32ptr - the memory offset to load the result into (bytes)
	/// codeOffset i32 - the offset within the code
	/// length i32 - the length of code to copy
	///
	/// Returns: nothing
	///
	/// Trap conditions:
	///
	/// load address from memory at addressOffset results in out of bounds access,
	/// load length number of bytes from the account code buffer at codeOffset results in out of bounds access,
	/// store length number of bytes to memory at resultOffset results in out of bounds access.
	pub fn external_code_copy(&mut self, args: RuntimeArgs) -> Result<()> {
		let address = self.address_at(args.nth_checked(0)?)?;
		let result_ptr: u32 = args.nth_checked(1)?;
		let code_ptr: u32 = args.nth_checked(2)?;
		let code_len: u32 = args.nth_checked(3)?;

		if let Ok(Some(code)) = self.ext.extcode(&address) {
			let code_range = code_ptr as usize..(code_ptr + code_len) as usize;
			self.memory.set(result_ptr, &code[code_range])?;
			Ok(())
		} else {
			let msg = format!("couldn't fetch code at address {}", address);
			Err(Error::Panic(msg).into())
		}
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


mod ext_impl {

        use wasmi::{Externals, RuntimeArgs, RuntimeValue, Trap};
	use env::ids::*;

        macro_rules! void {
	        { $e: expr } => { { $e?; Ok(None) } }
        }

        macro_rules! some {
	        { $e: expr } => { { Ok(Some($e?)) } }
        }

        macro_rules! cast {
	        { $e: expr } => { { Ok(Some($e)) } }
        }

        impl<'a> Externals for super::Runtime<'a> {

	        fn invoke_index(
		        &mut self,
		        index: usize,
		        args: RuntimeArgs
	        ) -> std::result::Result<Option<RuntimeValue>, Trap> {
		        match index {
			        USE_GAS_FUNC => void!(self.use_gas(args)),
			        GET_GAS_LEFT_FUNC => some!(self.get_gas_left()),
			        GET_ADDRESS_FUNC => void!(self.get_address(args)),
			        GET_EXTERNAL_BALANCE_FUNC => void!(self.get_external_balance(args)),
			        GET_BLOCK_COINBASE_FUNC => void!(self.get_block_coinbase(args)),
			        GET_BLOCK_DIFFICULTY_FUNC => void!(self.get_block_difficulty(args)),
			        GET_BLOCK_GAS_LIMIT_FUNC => some!(self.get_block_gas_limit()),
			        GET_BLOCK_HASH_FUNC => some!(self.get_block_hash(args)),
			        GET_BLOCK_NUMBER_FUNC => some!(self.get_block_number()),
			        GET_BLOCK_TIMESTAMP_FUNC => some!(self.get_block_timestamp()),
			        GET_TX_GAS_PRICE_FUNC => void!(self.get_tx_gas_price(args)),
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
			        CODE_COPY_FUNC => void!(self.code_copy(args)),
			        GET_CODE_SIZE_FUNC => some!(self.get_code_size()),
			        EXTERNAL_CODE_COPY_FUNC => void!(self.external_code_copy(args)),
			        GET_EXTERNAL_CODE_SIZE_FUNC => some!(self.get_external_code_size(args)),
			        STORAGE_LOAD_FUNC => void!(self.storage_load(args)),
			        STORAGE_STORE_FUNC => void!(self.storage_store(args)),
			        SELF_DESTRUCT_FUNC => void!(self.self_destruct(args)),
			        _ => panic!("ethereum module doesn't provide function at index {}", index)
		        }
	        }
        }
}
