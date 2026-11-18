//! Repeated fields that gRPC request messages can carry, read from a service's
//! file descriptor set.
//!
//! Every public gRPC server bounds the element count of these fields before
//! decoding (`zinder_runtime::RepeatedFieldGuard`). Tests compare each
//! server's limit table against [`repeated_request_fields`], so a new repeated
//! request field, or a renumbered one, fails a test until it is bounded.

use std::collections::{BTreeSet, HashMap};

use eyre::{Result, eyre};
use prost::Message as _;
use prost_types::{
    DescriptorProto, FileDescriptorSet,
    field_descriptor_proto::{Label, Type},
};

/// One repeated field reachable from a gRPC method's request message.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RepeatedRequestField {
    /// Method name within the service, for example `GetTaddressBalance`.
    pub method: String,
    /// Dotted protobuf field names from the request message down to the
    /// repeated field.
    pub field_name: String,
    /// Field numbers matching `field_name`.
    pub field_path: Vec<u32>,
    /// Whether each element is a varint (enum, integer, or `bool`) rather than
    /// a length-delimited record (`string`, `bytes`, or message).
    pub varint_elements: bool,
}

/// Lists every repeated field reachable from each request message of
/// `service`, through singular message fields, sorted.
///
/// `service` is the fully qualified service name, such as
/// `cash.z.wallet.sdk.rpc.CompactTxStreamer`. Repeated fields inside the
/// elements of a repeated message field are not listed: bounding the outer
/// field bounds them.
pub fn repeated_request_fields(
    file_descriptor_set: &[u8],
    service: &str,
) -> Result<Vec<RepeatedRequestField>> {
    let descriptor_set = FileDescriptorSet::decode(file_descriptor_set)?;
    let mut messages = HashMap::new();
    for file in &descriptor_set.file {
        let package = file.package();
        for message in &file.message_type {
            index_message(&mut messages, &format!(".{package}"), message);
        }
    }
    let service_descriptor = descriptor_set
        .file
        .iter()
        .flat_map(|file| {
            file.service
                .iter()
                .map(move |candidate| (file.package(), candidate))
        })
        .find(|(package, candidate)| format!("{package}.{}", candidate.name()) == service)
        .map(|(_, candidate)| candidate)
        .ok_or_else(|| eyre!("descriptor set has no service {service}"))?;

    let mut fields = BTreeSet::new();
    for method in &service_descriptor.method {
        let mut walk = RequestWalk {
            messages: &messages,
            method: method.name(),
            fields: &mut fields,
            visiting: Vec::new(),
        };
        walk.visit(method.input_type(), "", &[])?;
    }
    Ok(fields.into_iter().collect())
}

fn index_message<'descriptor>(
    messages: &mut HashMap<String, &'descriptor DescriptorProto>,
    scope: &str,
    message: &'descriptor DescriptorProto,
) {
    let full_name = format!("{scope}.{}", message.name());
    for nested in &message.nested_type {
        index_message(messages, &full_name, nested);
    }
    messages.insert(full_name, message);
}

struct RequestWalk<'walk> {
    messages: &'walk HashMap<String, &'walk DescriptorProto>,
    method: &'walk str,
    fields: &'walk mut BTreeSet<RepeatedRequestField>,
    /// Message types on the current path, so recursive messages terminate.
    visiting: Vec<String>,
}

impl RequestWalk<'_> {
    fn visit(&mut self, type_name: &str, name_prefix: &str, path_prefix: &[u32]) -> Result<()> {
        if self.visiting.iter().any(|visiting| visiting == type_name) {
            return Ok(());
        }
        let message = self
            .messages
            .get(type_name)
            .ok_or_else(|| eyre!("descriptor set has no message {type_name}"))?;
        self.visiting.push(type_name.to_owned());
        for field in &message.field {
            let field_name = format!("{name_prefix}{}", field.name());
            let number = u32::try_from(field.number())?;
            let field_path = [path_prefix, &[number]].concat();
            if field.label() == Label::Repeated {
                self.fields.insert(RepeatedRequestField {
                    method: self.method.to_owned(),
                    field_name,
                    field_path,
                    varint_elements: varint_elements(field.r#type())?,
                });
            } else if field.r#type() == Type::Message {
                self.visit(field.type_name(), &format!("{field_name}."), &field_path)?;
            }
        }
        self.visiting.pop();
        Ok(())
    }
}

fn varint_elements(field_type: Type) -> Result<bool> {
    match field_type {
        Type::String | Type::Bytes | Type::Message | Type::Group => Ok(false),
        Type::Enum
        | Type::Bool
        | Type::Int32
        | Type::Int64
        | Type::Uint32
        | Type::Uint64
        | Type::Sint32
        | Type::Sint64 => Ok(true),
        Type::Double
        | Type::Float
        | Type::Fixed32
        | Type::Fixed64
        | Type::Sfixed32
        | Type::Sfixed64 => Err(eyre!(
            "repeated fixed-width request fields are not supported: {field_type:?}"
        )),
    }
}
