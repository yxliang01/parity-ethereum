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

use std::path::Path;
use std::io;
use std::sync::{Arc, RwLock, RwLockReadGuard};

use kvdb::{KeyValueDB, DBTransaction, DBValue, DBOp};
use super::rkv::{Manager, Rkv, Reader, Store, Value, Iter};

use super::kvdb_rocksdb::{DatabaseConfig};

fn other_io_err<E>(e: E) -> io::Error where E: ToString {
	io::Error::new(io::ErrorKind::Other, e.to_string())
}

pub struct Database {
	manager: Arc<RwLock<Rkv>>,
}

impl Database {
	/// Open database with default settings.
	pub fn open_default(path: &str) -> io::Result<Database> {
		Database::open(&DatabaseConfig::default(), path)
	}

	/// Open database file. Creates if it does not exist.
	pub fn open(_config: &DatabaseConfig, path: &str) -> io::Result<Database> {
		let manager = Manager::singleton().write().unwrap()
			.get_or_create(Path::new(path), Rkv::new).map_err(other_io_err)?;

		Ok(Database{
			manager,
		})
	}

	fn open_store(manager: &Arc<RwLock<Rkv>>, col: Option<u32>) -> io::Result<Store> {
		let env = manager.read().unwrap();
		let store = match col {
			None => env.open_or_create(None),
			Some(col_value) => {
				let db_name = &col_value.to_string()[..];
				env.open_or_create(db_name)
			}
		};

		store.map_err(other_io_err)
	}

	/// Get value by key.
	pub fn get(&self, col: Option<u32>, key: &[u8]) -> io::Result<Option<DBValue>> {
		let store = Database::open_store(&self.manager, col)?;
		let env = self.manager.read().unwrap();
		let reader = env.read().unwrap();

		let result = reader.get(store, key).map_err(other_io_err)?
			.map(|value| DBValue::from_slice(&value.to_bytes().unwrap()));

		Ok(result)
	}

	/// Get value by partial key. Prefix size should match configured prefix size. Only searches flushed values.
	// TODO: support prefix seek for unflushed data
	pub fn get_by_prefix(&self, _col: Option<u32>, _prefix: &[u8]) -> Option<Box<[u8]>> {
		// self.iter_from_prefix(col, prefix).and_then(|mut iter| {
		// 	match iter.next() {
		// 		// TODO: use prefix_same_as_start read option (not availabele in C API currently)
		// 		Some((k, v)) => if k[0 .. prefix.len()] == prefix[..] { Some(v) } else { None },
		// 		_ => None
		// 	}
		// })
		error!(target: "lmdb", "get_by_prefix not implemented.");
		None
	}

	/// Commit transaction to database.
	pub fn write_buffered(&self, tr: DBTransaction) {
		self.write(tr).unwrap();
	}

	fn write(&self, transaction: DBTransaction) -> io::Result<()> {
		let env = self.manager.read().unwrap();

		for op in transaction.ops {
			match op {
				DBOp::Insert { col, key, value } => {
					let store = Database::open_store(&self.manager, col)?;
					let mut writer = env.write().unwrap();

					writer.put(store, key, &Value::Blob(&value)).map_err(other_io_err)?;
					writer.commit().map_err(other_io_err)?;
				},
				DBOp::Delete { col, key } => {
					let store = Database::open_store(&self.manager, col)?;
					let mut writer = env.write().unwrap();

					writer.delete(store, key).map_err(other_io_err)?;
					writer.commit().map_err(other_io_err)?;
				}
			}
		}

		Ok(())
	}

	fn flush(&self) -> io::Result<()> {
		Ok(())
	}

	fn iter(&self, col: Option<u32>) -> DatabaseIterator {
		let store = Database::open_store(&self.manager, col).unwrap();
		let env = self.manager.read().unwrap();
		let reader: Reader<&[u8]> = env.read().unwrap();

		DatabaseIterator::new(reader.iter_start(store).unwrap())
	}

	fn iter_from_prefix(&self, col: Option<u32>, _prefix: &[u8]) -> DatabaseIterator {
		self.iter(col)
	}

	fn restore(&self, _new_db: &str) -> io::Result<()> {
		error!(target: "lmdb", "restore not yet implemented.");
		Ok(())
	}
}

struct DatabaseIterator<'env> {
	inner: Iter<'env>,
}

impl<'env> DatabaseIterator<'env> {
	pub fn new(iter: Iter<'env>) -> Self {
		DatabaseIterator {
			inner: iter,
		}
	}
}

impl<'env> Iterator for DatabaseIterator<'env> {
	type Item = (Box<[u8]>, Box<[u8]>);

	fn next(&mut self) -> Option<Self::Item> {
		match self.inner.next() {
			None => None,
			Some((key, value)) => {
				Some((
					key.to_vec().into_boxed_slice(),
					value.unwrap().unwrap().to_bytes().unwrap().into_boxed_slice(),
				))
			},
		}
	}
}

// duplicate declaration of methods here to avoid trait import in certain existing cases
// at time of addition.
impl KeyValueDB for Database {
	fn get(&self, col: Option<u32>, key: &[u8]) -> io::Result<Option<DBValue>> {
		Database::get(self, col, key)
	}

	fn get_by_prefix(&self, col: Option<u32>, prefix: &[u8]) -> Option<Box<[u8]>> {
		Database::get_by_prefix(self, col, prefix)
	}

	fn write_buffered(&self, transaction: DBTransaction) {
		Database::write_buffered(self, transaction)
	}

	fn write(&self, transaction: DBTransaction) -> io::Result<()> {
		Database::write(self, transaction)
	}

	fn flush(&self) -> io::Result<()> {
		Database::flush(self)
	}

	fn iter<'a>(&'a self, col: Option<u32>) -> Box<Iterator<Item=(Box<[u8]>, Box<[u8]>)> + 'a> {
		let unboxed = Database::iter(self, col);
		Box::new(unboxed)
	}

	fn iter_from_prefix<'a>(&'a self, col: Option<u32>, prefix: &'a [u8])
		-> Box<Iterator<Item=(Box<[u8]>, Box<[u8]>)> + 'a>
	{
		let unboxed = Database::iter_from_prefix(self, col, prefix);
		Box::new(unboxed)
	}

	fn restore(&self, new_db: &str) -> io::Result<()> {
		Database::restore(self, new_db)
	}
}

impl Drop for Database {
	fn drop(&mut self) {
		// write all buffered changes if we can.
		let _ = self.flush();
	}
}
