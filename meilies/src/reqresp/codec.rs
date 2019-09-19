use std::{fmt, io};

use bytes::BytesMut;
use futures_codec::{Encoder, Decoder};

use crate::resp::{RespValue, FromResp, RespCodec, RespMsgError};
use super::{Request, Response, RespRequestConvertError, RespResponseConvertError};

#[derive(Debug, Default)]
pub struct ClientCodec;

impl Decoder for ClientCodec {
    type Item = Result<Response, String>;
    type Error = ResponseMsgError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match RespCodec.decode(buf)? {
            Some(value) => Ok(Some(FromResp::from_resp(value)?)),
            None => Ok(None),
        }
    }
}

impl Encoder for ClientCodec {
    type Item = Request;
    type Error = RequestMsgError;

    fn encode(&mut self, msg: Self::Item, buf: &mut BytesMut) -> Result<(), Self::Error> {
        Ok(RespCodec.encode(msg.into(), buf)?)
    }
}

#[derive(Debug, Default)]
pub struct ServerCodec;

impl Decoder for ServerCodec {
    type Item = Request;
    type Error = RequestMsgError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match RespCodec.decode(buf)? {
            Some(value) => Ok(Some(FromResp::from_resp(value)?)),
            None => Ok(None),
        }
    }
}

impl Encoder for ServerCodec {
    type Item = Result<Response, String>;
    type Error = ResponseMsgError;

    fn encode(&mut self, msg: Self::Item, buf: &mut BytesMut) -> Result<(), Self::Error> {
        let msg = match msg {
            Ok(item) => item.into(),
            Err(error) => RespValue::Error(error),
        };

        Ok(RespCodec.encode(msg, buf)?)
    }
}

#[derive(Debug)]
pub enum RequestMsgError {
    RequestMsgError(RespRequestConvertError),
    RespMsgError(RespMsgError),
}

impl fmt::Display for RequestMsgError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RequestMsgError::RequestMsgError(error) => write!(f, "{}", error),
            RequestMsgError::RespMsgError(error) => write!(f, "{}", error),
        }
    }
}

impl From<RespMsgError> for RequestMsgError {
    fn from(error: RespMsgError) -> RequestMsgError {
        RequestMsgError::RespMsgError(error)
    }
}

impl From<RespRequestConvertError> for RequestMsgError {
    fn from(error: RespRequestConvertError) -> RequestMsgError {
        RequestMsgError::RequestMsgError(error)
    }
}

impl From<io::Error> for RequestMsgError {
    fn from(error: io::Error) -> RequestMsgError {
        RequestMsgError::from(RespMsgError::from(error))
    }
}

#[derive(Debug)]
pub enum ResponseMsgError {
    ResponseMsgError(RespResponseConvertError),
    RespMsgError(RespMsgError),
}

impl fmt::Display for ResponseMsgError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ResponseMsgError::ResponseMsgError(error) => write!(f, "{}", error),
            ResponseMsgError::RespMsgError(error) => write!(f, "{}", error),
        }
    }
}

impl From<RespMsgError> for ResponseMsgError {
    fn from(error: RespMsgError) -> ResponseMsgError {
        ResponseMsgError::RespMsgError(error)
    }
}

impl From<RespResponseConvertError> for ResponseMsgError {
    fn from(error: RespResponseConvertError) -> ResponseMsgError {
        ResponseMsgError::ResponseMsgError(error)
    }
}

impl From<io::Error> for ResponseMsgError {
    fn from(error: io::Error) -> ResponseMsgError {
        ResponseMsgError::from(RespMsgError::from(error))
    }
}
