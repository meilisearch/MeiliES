// mod sub;
mod paired;

// pub use self::sub::{sub_connect, SubStream, SubController, ProtocolError};
pub use self::paired::{PairedConnection, PairedConnectionError};
