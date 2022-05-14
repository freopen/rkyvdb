use std::{marker::PhantomData, ops::Deref};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Collection is not registered in this DB")]
    CollectionNotRegistered,
    #[error("RocksDB error")]
    RocksDB(#[from] rocksdb::Error),
}

pub struct Database {
    rocksdb: rocksdb::DB,
    mutex: std::sync::Mutex<()>,
}

impl Database {
    pub fn new(path: &str) -> Result<Database, rocksdb::Error> {
        let opts = rocksdb::Options::default();
        Ok(Database {
            rocksdb: rocksdb::DB::open(&opts, path)?,
            mutex: std::sync::Mutex::new(()),
        })
    }
}

pub trait Key {
    fn serialize(&self) -> &[u8];
}

impl Key for () {
    fn serialize(&self) -> &[u8] {
        &[]
    }
}

impl Key for str {
    fn serialize(&self) -> &[u8] {
        self.as_bytes()
    }
}

struct CaseInsensitiveString(String);

impl From<&str> for CaseInsensitiveString {
    fn from(s: &str) -> Self {
        Self(s.to_lowercase())
    }
}

impl<'a> Key for CaseInsensitiveString {
    fn serialize(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

pub struct Value<'db, T: Collection> {
    bytes: rocksdb::DBPinnableSlice<'db>,
    phantom: PhantomData<T::Archived>,
}

impl<'db, T: Collection> Deref for Value<'db, T> {
    type Target = T::Archived;
    fn deref(&self) -> &Self::Target {
        unsafe { rkyv::archived_root::<T>(&self.bytes) }
    }
}

impl<'db, T: Collection> Value<'db, T> {
    pub fn deser(&self) -> T
    where
        <T as rkyv::Archive>::Archived:
            rkyv::Deserialize<T, rkyv::de::deserializers::SharedDeserializeMap>,
    {
        unsafe {
            rkyv::from_bytes_unchecked(&self.bytes).expect("Internal error: deserialization failed")
        }
    }
}

pub trait Collection:
    Sized
    + rkyv::Archive
    + rkyv::Deserialize<Self, rkyv::de::deserializers::SharedDeserializeMap>
    + rkyv::Serialize<rkyv::ser::serializers::AllocSerializer<1024>>
{
    type KeyType: Key;
    const CF_NAME: &'static str;

    fn key(&self) -> &Self::KeyType;

    fn get<K: Into<Self::KeyType>>(
        key: K,
        db: &Database,
    ) -> Result<Option<Value<'_, Self>>, Error> {
        let key = key.into();
        let cf = db
            .rocksdb
            .cf_handle(Self::CF_NAME)
            .ok_or(Error::CollectionNotRegistered)?;
        Ok(db
            .rocksdb
            .get_pinned_cf(cf, key.serialize())?
            .map(|v| Value {
                bytes: v,
                phantom: PhantomData,
            }))
    }

    fn modify<K: Into<Self::KeyType>>(
        key: K,
        modifier: impl FnOnce(&mut Option<Self>),
        db: &Database,
    ) -> Result<(), Error>
    where
        <Self as rkyv::Archive>::Archived:
            rkyv::Deserialize<Self, rkyv::de::deserializers::SharedDeserializeMap>,
    {
        let cf = db
            .rocksdb
            .cf_handle(Self::CF_NAME)
            .ok_or(Error::CollectionNotRegistered)?;
        let key: Self::KeyType = key.into();
        let serialized_key = key.serialize();
        let _guard = db.mutex.lock().unwrap();
        let mut value = db.rocksdb.get_pinned_cf(cf, serialized_key)?.map(|v| unsafe {
            rkyv::from_bytes_unchecked::<Self>(&v).expect("Internal error: deserialization failed")
        });
        modifier(&mut value);
        if let Some(value) = value {
            db.rocksdb.put_cf(
                cf,
                serialized_key,
                rkyv::to_bytes::<_, 1024>(&value)
                    .expect("Internal error: serialization failed")
                    .as_ref(),
            )?;
        } else {
            db.rocksdb.delete_cf(cf, serialized_key)?;
        }
        Ok(())
    }
}
