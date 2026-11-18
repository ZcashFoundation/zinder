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
/// `service`, through singular and repeated message fields, sorted.
///
/// `service` is the fully qualified service name, such as
/// `cash.z.wallet.sdk.rpc.CompactTxStreamer`. A repeated field inside the
/// elements of a repeated message field is listed too: bounding the outer
/// list does not bound how many elements one of its messages holds. Its
/// path runs through the outer field, and the guard counts its elements
/// across every outer element.
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
                    field_name: field_name.clone(),
                    field_path: field_path.clone(),
                    varint_elements: varint_elements(field.r#type())?,
                });
            }
            if field.r#type() == Type::Message {
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

#[cfg(test)]
mod tests {
    use prost::Message as _;
    use prost_types::{
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
        MethodDescriptorProto, ServiceDescriptorProto,
        field_descriptor_proto::{Label, Type},
    };

    use super::{RepeatedRequestField, repeated_request_fields};

    fn field(name: &str, number: i32, label: Label, field_type: Type) -> FieldDescriptorProto {
        FieldDescriptorProto {
            name: Some(name.to_owned()),
            number: Some(number),
            label: Some(label.into()),
            r#type: Some(field_type.into()),
            type_name: (field_type == Type::Message).then(|| ".probe.Element".to_owned()),
            ..FieldDescriptorProto::default()
        }
    }

    fn listed(field_name: &str, field_path: &[u32], varint_elements: bool) -> RepeatedRequestField {
        RepeatedRequestField {
            method: "Probe".to_owned(),
            field_name: field_name.to_owned(),
            field_path: field_path.to_vec(),
            varint_elements,
        }
    }

    /// A list inside each element of a repeated message is listed with its
    /// path through the outer list, and a message that repeats itself
    /// terminates the walk.
    #[test]
    fn lists_repeated_fields_inside_repeated_message_elements() -> eyre::Result<()> {
        let request = DescriptorProto {
            name: Some("Request".to_owned()),
            field: vec![field("elements", 1, Label::Repeated, Type::Message)],
            ..DescriptorProto::default()
        };
        let element = DescriptorProto {
            name: Some("Element".to_owned()),
            field: vec![
                field("tags", 1, Label::Repeated, Type::String),
                field("pools", 2, Label::Repeated, Type::Uint32),
                field("children", 3, Label::Repeated, Type::Message),
            ],
            ..DescriptorProto::default()
        };
        let service = ServiceDescriptorProto {
            name: Some("ProbeService".to_owned()),
            method: vec![MethodDescriptorProto {
                name: Some("Probe".to_owned()),
                input_type: Some(".probe.Request".to_owned()),
                output_type: Some(".probe.Request".to_owned()),
                ..MethodDescriptorProto::default()
            }],
            ..ServiceDescriptorProto::default()
        };
        let descriptor_set = FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some("probe.proto".to_owned()),
                package: Some("probe".to_owned()),
                message_type: vec![request, element],
                service: vec![service],
                ..FileDescriptorProto::default()
            }],
        };

        let fields =
            repeated_request_fields(&descriptor_set.encode_to_vec(), "probe.ProbeService")?;

        assert_eq!(
            fields,
            vec![
                listed("elements", &[1], false),
                listed("elements.children", &[1, 3], false),
                listed("elements.pools", &[1, 2], true),
                listed("elements.tags", &[1, 1], false),
            ]
        );
        Ok(())
    }
}
