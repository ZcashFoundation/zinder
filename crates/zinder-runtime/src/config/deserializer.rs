//! Type-directed `serde::Deserializer` over [`ConfigNode`] with env-var provenance.

use std::{collections::btree_map, fmt, slice, str::FromStr};

use serde::de::{
    self, DeserializeOwned, DeserializeSeed, Deserializer, EnumAccess, MapAccess, SeqAccess,
    VariantAccess, Visitor,
};

use super::{
    ConfigError, suggest_env_var,
    tree::{ConfigNode, EnvOrigin, PrimitiveValue},
};

pub(crate) fn deserialize_tree<T>(root: &ConfigNode) -> Result<T, ConfigError>
where
    T: DeserializeOwned,
{
    let deserializer = NodeDeserializer {
        node: NodeRef::Node(root),
        path: String::new(),
    };
    T::deserialize(deserializer).map_err(DeserError::into_config_error)
}

#[derive(Clone, Debug)]
struct DeserError {
    kind: DeserErrorKind,
}

#[derive(Clone, Debug)]
enum DeserErrorKind {
    UnknownField {
        field: String,
        expected: &'static [&'static str],
        parent_path: String,
        env_origin: Option<EnvOrigin>,
        enriched: bool,
    },
    InvalidEnvValue {
        origin: EnvOrigin,
        reason: String,
    },
    Message {
        path: Option<String>,
        env_origin: Option<EnvOrigin>,
        message: String,
    },
}

impl DeserError {
    fn invalid_env(origin: &EnvOrigin, reason: impl Into<String>) -> Self {
        Self {
            kind: DeserErrorKind::InvalidEnvValue {
                origin: origin.clone(),
                reason: reason.into(),
            },
        }
    }

    fn message(
        path: Option<String>,
        env_origin: Option<EnvOrigin>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind: DeserErrorKind::Message {
                path,
                env_origin,
                message: message.into(),
            },
        }
    }

    fn enrich_key(&mut self, parent: &str, env_origin_ref: Option<&EnvOrigin>) {
        if let DeserErrorKind::UnknownField {
            parent_path,
            env_origin,
            enriched,
            ..
        } = &mut self.kind
            && !*enriched
        {
            *enriched = true;
            parent.clone_into(parent_path);
            *env_origin = env_origin_ref.cloned();
        }
    }

    fn enrich_value(&mut self, field_path: &str, env_origin_ref: Option<&EnvOrigin>) {
        if let DeserErrorKind::Message {
            path, env_origin, ..
        } = &mut self.kind
            && path.is_none()
        {
            *path = Some(field_path.to_owned());
            if env_origin.is_none() {
                *env_origin = env_origin_ref.cloned();
            }
        }
    }

    fn enrich_node_value(&mut self, field_path: &str, entry_node: &ConfigNode) {
        if let DeserErrorKind::Message {
            path,
            env_origin,
            message,
        } = &mut self.kind
            && path.is_none()
        {
            if let Some(origin) = direct_env_origin(entry_node) {
                *path = Some(field_path.to_owned());
                if env_origin.is_none() {
                    *env_origin = Some(origin.clone());
                }
            } else if let Some((leaf_path, leaf_origin)) =
                entry_node.find_mentioned_leaf(field_path, message)
            {
                *path = Some(leaf_path);
                if env_origin.is_none() {
                    *env_origin = leaf_origin.cloned();
                }
            } else {
                *path = Some(field_path.to_owned());
            }
        }
    }

    fn into_config_error(self) -> ConfigError {
        match self.kind {
            DeserErrorKind::UnknownField {
                field,
                expected,
                parent_path,
                env_origin: Some(origin),
                ..
            } => {
                let detail = format_unknown_field(&field, expected, &parent_path);
                let suggested_name = suggest_env_var(&origin.var_name, &origin.path);
                ConfigError::RejectedEnvVar {
                    original_name: origin.var_name,
                    rejected_key: origin.path,
                    suggested_name,
                    detail: Some(detail.into_boxed_str()),
                }
            }
            DeserErrorKind::UnknownField {
                field,
                expected,
                parent_path,
                env_origin: None,
                ..
            } => {
                let full_path = join_path(&parent_path, &field);
                let reason = format_unknown_field(&field, expected, &parent_path);
                ConfigError::Deserialize {
                    path: Some(full_path),
                    reason,
                }
            }
            DeserErrorKind::InvalidEnvValue { origin, reason } => ConfigError::InvalidEnvValue {
                original_name: origin.var_name,
                key: origin.path,
                reason,
            },
            DeserErrorKind::Message {
                path,
                env_origin: Some(origin),
                message,
            } => ConfigError::InvalidEnvValue {
                original_name: origin.var_name,
                key: path.unwrap_or(origin.path),
                reason: message,
            },
            DeserErrorKind::Message {
                path,
                env_origin: None,
                message,
            } => ConfigError::Deserialize {
                path,
                reason: message,
            },
        }
    }
}

impl fmt::Display for DeserError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            DeserErrorKind::UnknownField {
                field,
                expected,
                parent_path,
                ..
            } => formatter.write_str(&format_unknown_field(field, expected, parent_path)),
            DeserErrorKind::InvalidEnvValue { origin, reason } => {
                write!(formatter, "{} ({}): {reason}", origin.var_name, origin.path)
            }
            DeserErrorKind::Message { message, .. } => formatter.write_str(message),
        }
    }
}

impl std::error::Error for DeserError {}

impl de::Error for DeserError {
    fn custom<T: fmt::Display>(message: T) -> Self {
        Self::message(None, None, message.to_string())
    }

    fn unknown_field(field: &str, expected: &'static [&'static str]) -> Self {
        Self {
            kind: DeserErrorKind::UnknownField {
                field: field.to_owned(),
                expected,
                parent_path: String::new(),
                env_origin: None,
                enriched: false,
            },
        }
    }
}

fn format_unknown_field(field: &str, expected: &[&str], parent_path: &str) -> String {
    let expected_clause = format_expected_fields(expected);
    let suffix = if parent_path.is_empty() {
        String::new()
    } else {
        format!(" for key `{parent_path}`")
    };
    format!("unknown field `{field}`, {expected_clause}{suffix}")
}

fn format_expected_fields(expected: &[&str]) -> String {
    match expected {
        [] => "there are no fields".to_owned(),
        [single] => format!("expected `{single}`"),
        [first, second] => format!("expected `{first}` or `{second}`"),
        many => {
            let quoted: Vec<String> = many.iter().map(|field| format!("`{field}`")).collect();
            format!("expected one of {}", quoted.join(", "))
        }
    }
}

fn join_path(parent: &str, segment: &str) -> String {
    if parent.is_empty() {
        segment.to_owned()
    } else {
        format!("{parent}.{segment}")
    }
}

#[derive(Clone, Copy)]
enum NodeRef<'de> {
    Node(&'de ConfigNode),
    Toml(&'de toml::Value),
    EnvSegment {
        origin: &'de EnvOrigin,
        segment: &'de str,
    },
    DefaultStr(&'de str),
}

struct ExpectedDisplay<'a>(&'a dyn de::Expected);

impl fmt::Display for ExpectedDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl<'de> NodeRef<'de> {
    fn env_origin(self) -> Option<&'de EnvOrigin> {
        match self {
            Self::Node(node) => node.first_env_origin(),
            Self::EnvSegment { origin, .. } => Some(origin),
            Self::Toml(_) | Self::DefaultStr(_) => None,
        }
    }

    fn type_mismatch(self, path: &str, expected: &dyn de::Expected) -> DeserError {
        let actual = self.kind_label();
        let expected_display = ExpectedDisplay(expected);
        self.env_origin().map_or_else(
            || {
                DeserError::message(
                    (!path.is_empty()).then(|| path.to_owned()),
                    None,
                    format!("invalid type: {actual}, expected {expected_display}"),
                )
            },
            |origin| {
                DeserError::invalid_env(
                    origin,
                    format!("expected {expected_display}, found {actual}"),
                )
            },
        )
    }

    fn kind_label(self) -> &'static str {
        match self {
            Self::Node(ConfigNode::Table(_)) | Self::Toml(toml::Value::Table(_)) => "a table",
            Self::Node(ConfigNode::Default(p) | ConfigNode::Override(p)) => p.kind_label(),
            Self::Node(ConfigNode::Toml(toml_val)) | Self::Toml(toml_val) => {
                toml_kind_label(toml_val)
            }
            Self::Node(ConfigNode::Env { .. }) | Self::EnvSegment { .. } | Self::DefaultStr(_) => {
                "a string"
            }
        }
    }
}

impl PrimitiveValue {
    const fn kind_label(&self) -> &'static str {
        match self {
            Self::Bool(_) => "a boolean",
            Self::Signed(_) | Self::Unsigned(_) => "an integer",
            Self::Float(_) => "a floating point number",
            Self::String(_) => "a string",
            Self::List(_) => "a sequence",
        }
    }
}

const fn toml_kind_label(toml_val: &toml::Value) -> &'static str {
    match toml_val {
        toml::Value::String(_) => "a string",
        toml::Value::Integer(_) => "an integer",
        toml::Value::Float(_) => "a floating point number",
        toml::Value::Boolean(_) => "a boolean",
        toml::Value::Datetime(_) => "a datetime",
        toml::Value::Array(_) => "an array",
        toml::Value::Table(_) => "a table",
    }
}

fn empty_env_error(origin: &EnvOrigin) -> DeserError {
    DeserError::invalid_env(
        origin,
        "empty value is not allowed; unset the variable to use the default or supply a valid value",
    )
}

struct NodeDeserializer<'de> {
    node: NodeRef<'de>,
    path: String,
}

impl<'de> NodeDeserializer<'de> {
    fn deserialize_signed<T, V>(self, expected: &str, visitor: V) -> Result<V::Value, DeserError>
    where
        T: FromStr + TryFrom<i64> + TryFrom<u64> + IntoSignedVisitor<'de, V>,
        V: Visitor<'de>,
    {
        let parsed = self.parse_signed::<T>(expected)?;
        parsed.visit_into(visitor)
    }

    fn parse_signed<T>(&self, expected: &str) -> Result<T, DeserError>
    where
        T: FromStr + TryFrom<i64> + TryFrom<u64>,
    {
        match self.node {
            NodeRef::Node(ConfigNode::Env { origin, raw }) => {
                if raw.trim().is_empty() {
                    return Err(empty_env_error(origin));
                }
                raw.trim()
                    .parse::<T>()
                    .map_err(|_| DeserError::invalid_env(origin, format!("expected {expected}")))
            }
            NodeRef::EnvSegment { origin, segment } => segment
                .trim()
                .parse::<T>()
                .map_err(|_| DeserError::invalid_env(origin, format!("expected {expected}"))),
            NodeRef::Node(ConfigNode::Default(p) | ConfigNode::Override(p)) => {
                primitive_to_signed(p, &self.path, expected)
            }
            NodeRef::Node(ConfigNode::Toml(toml::Value::Integer(n)))
            | NodeRef::Toml(toml::Value::Integer(n)) => T::try_from(*n).map_err(|_| {
                DeserError::message(
                    Some(self.path.clone()),
                    None,
                    format!("out of range for {expected}"),
                )
            }),
            other @ (NodeRef::Node(_) | NodeRef::Toml(_) | NodeRef::DefaultStr(_)) => {
                Err(other.type_mismatch(&self.path, &expected))
            }
        }
    }

    fn deserialize_unsigned<T, V>(self, expected: &str, visitor: V) -> Result<V::Value, DeserError>
    where
        T: FromStr + TryFrom<i64> + TryFrom<u64> + IntoUnsignedVisitor<'de, V>,
        V: Visitor<'de>,
    {
        let parsed = self.parse_unsigned::<T>(expected)?;
        parsed.visit_into(visitor)
    }

    fn parse_unsigned<T>(&self, expected: &str) -> Result<T, DeserError>
    where
        T: FromStr + TryFrom<i64> + TryFrom<u64>,
    {
        match self.node {
            NodeRef::Node(ConfigNode::Env { origin, raw }) => {
                if raw.trim().is_empty() {
                    return Err(empty_env_error(origin));
                }
                raw.trim()
                    .parse::<T>()
                    .map_err(|_| DeserError::invalid_env(origin, format!("expected {expected}")))
            }
            NodeRef::EnvSegment { origin, segment } => segment
                .trim()
                .parse::<T>()
                .map_err(|_| DeserError::invalid_env(origin, format!("expected {expected}"))),
            NodeRef::Node(ConfigNode::Default(p) | ConfigNode::Override(p)) => {
                primitive_to_unsigned(p, &self.path, expected)
            }
            NodeRef::Node(ConfigNode::Toml(toml::Value::Integer(n)))
            | NodeRef::Toml(toml::Value::Integer(n)) => T::try_from(*n).map_err(|_| {
                DeserError::message(
                    Some(self.path.clone()),
                    None,
                    format!("out of range for {expected}"),
                )
            }),
            other @ (NodeRef::Node(_) | NodeRef::Toml(_) | NodeRef::DefaultStr(_)) => {
                Err(other.type_mismatch(&self.path, &expected))
            }
        }
    }

    fn deserialize_string_like<V>(self, visitor: V) -> Result<V::Value, DeserError>
    where
        V: Visitor<'de>,
    {
        match self.node {
            NodeRef::Node(ConfigNode::Env { origin, raw }) => {
                if raw.trim().is_empty() {
                    return Err(empty_env_error(origin));
                }
                visitor.visit_str(raw.as_str())
            }
            NodeRef::EnvSegment { segment, .. } | NodeRef::DefaultStr(segment) => {
                visitor.visit_str(segment)
            }
            NodeRef::Node(
                ConfigNode::Default(PrimitiveValue::String(text))
                | ConfigNode::Override(PrimitiveValue::String(text))
                | ConfigNode::Toml(toml::Value::String(text)),
            )
            | NodeRef::Toml(toml::Value::String(text)) => visitor.visit_str(text.as_str()),
            other @ (NodeRef::Node(_) | NodeRef::Toml(_)) => {
                Err(other.type_mismatch(&self.path, &visitor))
            }
        }
    }

    fn deserialize_float_value<V>(self, as_f32: bool, visitor: V) -> Result<V::Value, DeserError>
    where
        V: Visitor<'de>,
    {
        let label = if as_f32 {
            "32-bit float"
        } else {
            "64-bit float"
        };
        let float_val = self.parse_f64(label)?;
        if as_f32 {
            let trimmed = match self.node {
                NodeRef::Node(ConfigNode::Env { origin, raw }) => {
                    if raw.trim().is_empty() {
                        return Err(empty_env_error(origin));
                    }
                    raw.trim()
                        .parse::<f32>()
                        .map_err(|_| DeserError::invalid_env(origin, format!("expected {label}")))?
                }
                NodeRef::EnvSegment { origin, segment } => segment
                    .trim()
                    .parse::<f32>()
                    .map_err(|_| DeserError::invalid_env(origin, format!("expected {label}")))?,
                NodeRef::Node(_) | NodeRef::Toml(_) | NodeRef::DefaultStr(_) => {
                    #[allow(
                        clippy::cast_possible_truncation,
                        reason = "ConfigNode float storage uses f64 internally"
                    )]
                    let narrowed = float_val as f32;
                    narrowed
                }
            };
            visitor.visit_f32(trimmed)
        } else {
            visitor.visit_f64(float_val)
        }
    }

    fn parse_f64(&self, label: &str) -> Result<f64, DeserError> {
        match self.node {
            NodeRef::Node(ConfigNode::Env { origin, raw }) => {
                if raw.trim().is_empty() {
                    return Err(empty_env_error(origin));
                }
                raw.trim()
                    .parse::<f64>()
                    .map_err(|_| DeserError::invalid_env(origin, format!("expected {label}")))
            }
            NodeRef::EnvSegment { origin, segment } => segment
                .trim()
                .parse::<f64>()
                .map_err(|_| DeserError::invalid_env(origin, format!("expected {label}"))),
            NodeRef::Node(ConfigNode::Default(p) | ConfigNode::Override(p)) => match p {
                PrimitiveValue::Float(num) => Ok(*num),
                PrimitiveValue::Signed(num) => {
                    #[allow(
                        clippy::cast_precision_loss,
                        reason = "serde integer-to-float widening matches Deserializer semantics"
                    )]
                    let widened = *num as f64;
                    Ok(widened)
                }
                PrimitiveValue::Unsigned(num) => {
                    #[allow(
                        clippy::cast_precision_loss,
                        reason = "serde integer-to-float widening matches Deserializer semantics"
                    )]
                    let widened = *num as f64;
                    Ok(widened)
                }
                PrimitiveValue::Bool(_) | PrimitiveValue::String(_) | PrimitiveValue::List(_) => {
                    Err(self.node.type_mismatch(&self.path, &label))
                }
            },
            NodeRef::Node(ConfigNode::Toml(toml::Value::Float(num)))
            | NodeRef::Toml(toml::Value::Float(num)) => Ok(*num),
            NodeRef::Node(ConfigNode::Toml(toml::Value::Integer(num)))
            | NodeRef::Toml(toml::Value::Integer(num)) => {
                #[allow(
                    clippy::cast_precision_loss,
                    reason = "TOML integers widen cleanly into float fields matching toml::Value"
                )]
                let widened = *num as f64;
                Ok(widened)
            }
            other @ (NodeRef::Node(_) | NodeRef::Toml(_) | NodeRef::DefaultStr(_)) => {
                Err(other.type_mismatch(&self.path, &label))
            }
        }
    }
}

fn primitive_to_signed<T: TryFrom<i64> + TryFrom<u64>>(
    primitive: &PrimitiveValue,
    path: &str,
    expected: &str,
) -> Result<T, DeserError> {
    match primitive {
        PrimitiveValue::Signed(num) => T::try_from(*num).map_err(|_| {
            DeserError::message(
                Some(path.to_owned()),
                None,
                format!("out of range for {expected}"),
            )
        }),
        PrimitiveValue::Unsigned(num) => T::try_from(*num).map_err(|_| {
            DeserError::message(
                Some(path.to_owned()),
                None,
                format!("out of range for {expected}"),
            )
        }),
        PrimitiveValue::Bool(_)
        | PrimitiveValue::Float(_)
        | PrimitiveValue::String(_)
        | PrimitiveValue::List(_) => Err(DeserError::message(
            (!path.is_empty()).then(|| path.to_owned()),
            None,
            format!(
                "invalid type: {}, expected {expected}",
                primitive.kind_label()
            ),
        )),
    }
}

fn primitive_to_unsigned<T: TryFrom<i64> + TryFrom<u64>>(
    primitive: &PrimitiveValue,
    path: &str,
    expected: &str,
) -> Result<T, DeserError> {
    primitive_to_signed(primitive, path, expected)
}

trait IntoSignedVisitor<'de, V: Visitor<'de>> {
    fn visit_into(self, visitor: V) -> Result<V::Value, DeserError>;
}

impl<'de, V: Visitor<'de>> IntoSignedVisitor<'de, V> for i8 {
    fn visit_into(self, visitor: V) -> Result<V::Value, DeserError> {
        visitor.visit_i8(self)
    }
}

impl<'de, V: Visitor<'de>> IntoSignedVisitor<'de, V> for i16 {
    fn visit_into(self, visitor: V) -> Result<V::Value, DeserError> {
        visitor.visit_i16(self)
    }
}

impl<'de, V: Visitor<'de>> IntoSignedVisitor<'de, V> for i32 {
    fn visit_into(self, visitor: V) -> Result<V::Value, DeserError> {
        visitor.visit_i32(self)
    }
}

impl<'de, V: Visitor<'de>> IntoSignedVisitor<'de, V> for i64 {
    fn visit_into(self, visitor: V) -> Result<V::Value, DeserError> {
        visitor.visit_i64(self)
    }
}

trait IntoUnsignedVisitor<'de, V: Visitor<'de>> {
    fn visit_into(self, visitor: V) -> Result<V::Value, DeserError>;
}

impl<'de, V: Visitor<'de>> IntoUnsignedVisitor<'de, V> for u8 {
    fn visit_into(self, visitor: V) -> Result<V::Value, DeserError> {
        visitor.visit_u8(self)
    }
}

impl<'de, V: Visitor<'de>> IntoUnsignedVisitor<'de, V> for u16 {
    fn visit_into(self, visitor: V) -> Result<V::Value, DeserError> {
        visitor.visit_u16(self)
    }
}

impl<'de, V: Visitor<'de>> IntoUnsignedVisitor<'de, V> for u32 {
    fn visit_into(self, visitor: V) -> Result<V::Value, DeserError> {
        visitor.visit_u32(self)
    }
}

impl<'de, V: Visitor<'de>> IntoUnsignedVisitor<'de, V> for u64 {
    fn visit_into(self, visitor: V) -> Result<V::Value, DeserError> {
        visitor.visit_u64(self)
    }
}

impl<'de> Deserializer<'de> for NodeDeserializer<'de> {
    type Error = DeserError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.node {
            NodeRef::Node(ConfigNode::Table(entries)) => {
                let mut access = ConfigTableAccess::new(entries, self.path);
                visitor.visit_map(&mut access)
            }
            NodeRef::Node(ConfigNode::Env { origin, raw }) => {
                if raw.trim().is_empty() {
                    return Err(empty_env_error(origin));
                }
                visitor.visit_str(raw.as_str())
            }
            NodeRef::EnvSegment { segment, .. } | NodeRef::DefaultStr(segment) => {
                visitor.visit_str(segment)
            }
            NodeRef::Node(ConfigNode::Default(primitive) | ConfigNode::Override(primitive)) => {
                deserialize_primitive_any(primitive, &self.path, visitor)
            }
            NodeRef::Node(ConfigNode::Toml(toml_val)) | NodeRef::Toml(toml_val) => {
                deserialize_toml_any(toml_val, &self.path, visitor)
            }
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.node {
            NodeRef::Node(ConfigNode::Env { origin, raw }) => {
                if raw.trim().is_empty() {
                    return Err(empty_env_error(origin));
                }
                match raw.as_str() {
                    "true" => visitor.visit_bool(true),
                    "false" => visitor.visit_bool(false),
                    _ => Err(DeserError::invalid_env(
                        origin,
                        "expected boolean (`true` or `false`)",
                    )),
                }
            }
            NodeRef::EnvSegment { origin, segment } => match segment {
                "true" => visitor.visit_bool(true),
                "false" => visitor.visit_bool(false),
                _ => Err(DeserError::invalid_env(
                    origin,
                    "expected boolean (`true` or `false`)",
                )),
            },
            NodeRef::Node(
                ConfigNode::Default(PrimitiveValue::Bool(flag))
                | ConfigNode::Override(PrimitiveValue::Bool(flag))
                | ConfigNode::Toml(toml::Value::Boolean(flag)),
            )
            | NodeRef::Toml(toml::Value::Boolean(flag)) => visitor.visit_bool(*flag),
            other @ (NodeRef::Node(_) | NodeRef::Toml(_) | NodeRef::DefaultStr(_)) => {
                Err(other.type_mismatch(&self.path, &visitor))
            }
        }
    }

    fn deserialize_i8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_signed::<i8, _>("signed 8-bit integer", visitor)
    }

    fn deserialize_i16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_signed::<i16, _>("signed 16-bit integer", visitor)
    }

    fn deserialize_i32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_signed::<i32, _>("signed 32-bit integer", visitor)
    }

    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_signed::<i64, _>("signed 64-bit integer", visitor)
    }

    fn deserialize_u8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_unsigned::<u8, _>("unsigned 8-bit integer", visitor)
    }

    fn deserialize_u16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_unsigned::<u16, _>("unsigned 16-bit integer", visitor)
    }

    fn deserialize_u32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_unsigned::<u32, _>("unsigned 32-bit integer", visitor)
    }

    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_unsigned::<u64, _>("unsigned 64-bit integer", visitor)
    }

    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_float_value(true, visitor)
    }

    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_float_value(false, visitor)
    }

    fn deserialize_char<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_str(visitor)
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_string_like(visitor)
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_string_like(visitor)
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.node {
            NodeRef::Node(ConfigNode::Env { origin, raw }) => {
                if raw.trim().is_empty() {
                    return Err(empty_env_error(origin));
                }
                visitor.visit_bytes(raw.as_bytes())
            }
            NodeRef::Node(ConfigNode::Toml(toml::Value::String(text)))
            | NodeRef::Toml(toml::Value::String(text)) => visitor.visit_bytes(text.as_bytes()),
            other @ (NodeRef::Node(_)
            | NodeRef::Toml(_)
            | NodeRef::EnvSegment { .. }
            | NodeRef::DefaultStr(_)) => Err(other.type_mismatch(&self.path, &visitor)),
        }
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_bytes(visitor)
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_some(self)
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        visitor.visit_unit()
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.node {
            NodeRef::Node(ConfigNode::Toml(toml::Value::Array(elements)))
            | NodeRef::Toml(toml::Value::Array(elements)) => {
                visitor.visit_seq(TomlSeqAccess::new(elements, &self.path))
            }
            NodeRef::Node(
                ConfigNode::Default(PrimitiveValue::List(elements))
                | ConfigNode::Override(PrimitiveValue::List(elements)),
            ) => visitor.visit_seq(DefaultSeqAccess::new(elements, &self.path)),
            NodeRef::Node(ConfigNode::Env { origin, raw }) => {
                if raw.trim().is_empty() {
                    return Err(empty_env_error(origin));
                }
                visitor.visit_seq(EnvSeqAccess::new(origin, raw.as_str(), &self.path))
            }
            other @ (NodeRef::Node(_)
            | NodeRef::Toml(_)
            | NodeRef::EnvSegment { .. }
            | NodeRef::DefaultStr(_)) => Err(other.type_mismatch(&self.path, &visitor)),
        }
    }

    fn deserialize_tuple<V: Visitor<'de>>(
        self,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.node {
            NodeRef::Node(ConfigNode::Table(entries)) => {
                let mut access = ConfigTableAccess::new(entries, self.path);
                visitor.visit_map(&mut access)
            }
            NodeRef::Node(ConfigNode::Toml(toml::Value::Table(entries)))
            | NodeRef::Toml(toml::Value::Table(entries)) => {
                let mut access = TomlTableAccess::new(entries, self.path);
                visitor.visit_map(&mut access)
            }
            other @ (NodeRef::Node(_)
            | NodeRef::Toml(_)
            | NodeRef::EnvSegment { .. }
            | NodeRef::DefaultStr(_)) => Err(other.type_mismatch(&self.path, &visitor)),
        }
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.deserialize_map(visitor)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        match self.node {
            NodeRef::Node(ConfigNode::Env { origin, raw }) => {
                if raw.trim().is_empty() {
                    return Err(empty_env_error(origin));
                }
                visitor.visit_enum(EnumStringAccess::new(raw.as_str(), Some(origin), variants))
            }
            NodeRef::EnvSegment { origin, segment } => {
                visitor.visit_enum(EnumStringAccess::new(segment, Some(origin), variants))
            }
            NodeRef::DefaultStr(text) => {
                visitor.visit_enum(EnumStringAccess::new(text, None, variants))
            }
            NodeRef::Node(
                ConfigNode::Default(PrimitiveValue::String(text))
                | ConfigNode::Override(PrimitiveValue::String(text))
                | ConfigNode::Toml(toml::Value::String(text)),
            )
            | NodeRef::Toml(toml::Value::String(text)) => {
                visitor.visit_enum(EnumStringAccess::new(text.as_str(), None, variants))
            }
            other @ (NodeRef::Node(_) | NodeRef::Toml(_)) => {
                Err(other.type_mismatch(&self.path, &visitor))
            }
        }
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_str(visitor)
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_unit()
    }
}

fn deserialize_primitive_any<'de, V: Visitor<'de>>(
    primitive: &'de PrimitiveValue,
    path: &str,
    visitor: V,
) -> Result<V::Value, DeserError> {
    match primitive {
        PrimitiveValue::Bool(flag) => visitor.visit_bool(*flag),
        PrimitiveValue::Signed(num) => visitor.visit_i64(*num),
        PrimitiveValue::Unsigned(num) => {
            if let Ok(signed) = i64::try_from(*num) {
                visitor.visit_i64(signed)
            } else {
                visitor.visit_u64(*num)
            }
        }
        PrimitiveValue::Float(num) => visitor.visit_f64(*num),
        PrimitiveValue::String(text) => visitor.visit_str(text.as_str()),
        PrimitiveValue::List(elements) => visitor.visit_seq(DefaultSeqAccess::new(elements, path)),
    }
}

fn deserialize_toml_any<'de, V: Visitor<'de>>(
    toml_val: &'de toml::Value,
    path: &str,
    visitor: V,
) -> Result<V::Value, DeserError> {
    match toml_val {
        toml::Value::String(text) => visitor.visit_str(text.as_str()),
        toml::Value::Integer(num) => visitor.visit_i64(*num),
        toml::Value::Float(num) => visitor.visit_f64(*num),
        toml::Value::Boolean(flag) => visitor.visit_bool(*flag),
        toml::Value::Datetime(dt) => visitor.visit_str(&dt.to_string()),
        toml::Value::Array(elements) => visitor.visit_seq(TomlSeqAccess::new(elements, path)),
        toml::Value::Table(entries) => {
            let mut access = TomlTableAccess::new(entries, path.to_owned());
            visitor.visit_map(&mut access)
        }
    }
}

fn direct_env_origin(node: &ConfigNode) -> Option<&EnvOrigin> {
    match node {
        ConfigNode::Env { origin, .. } => Some(origin),
        ConfigNode::Table(_)
        | ConfigNode::Default(_)
        | ConfigNode::Toml(_)
        | ConfigNode::Override(_) => None,
    }
}

struct ConfigTableAccess<'de> {
    iter: btree_map::Iter<'de, String, ConfigNode>,
    parent_path: String,
    current_entry: Option<&'de ConfigNode>,
    last_visited_path: Option<String>,
}

impl<'de> ConfigTableAccess<'de> {
    fn new(
        entries: &'de std::collections::BTreeMap<String, ConfigNode>,
        parent_path: String,
    ) -> Self {
        Self {
            iter: entries.iter(),
            parent_path,
            current_entry: None,
            last_visited_path: None,
        }
    }
}

impl<'de> MapAccess<'de> for ConfigTableAccess<'de> {
    type Error = DeserError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        let Some((entry_key, entry_node)) = self.iter.next() else {
            return Ok(None);
        };
        let full_path = join_path(&self.parent_path, entry_key);
        self.current_entry = Some(entry_node);
        self.last_visited_path = Some(full_path);
        let mut key_outcome = seed.deserialize(KeyDeserializer(entry_key.as_str()));
        if let Err(ref mut error) = key_outcome {
            error.enrich_key(&self.parent_path, entry_node.first_env_origin());
        }
        key_outcome.map(Some)
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let Some(entry_node) = self.current_entry.take() else {
            return Err(de::Error::custom("next_value_seed called without key"));
        };
        let visited_path = self.last_visited_path.clone().unwrap_or_default();
        let mut outcome = seed.deserialize(NodeDeserializer {
            node: NodeRef::Node(entry_node),
            path: visited_path.clone(),
        });
        if let Err(ref mut error) = outcome {
            error.enrich_node_value(&visited_path, entry_node);
        }
        outcome
    }
}

struct TomlTableAccess<'de> {
    iter: toml::map::Iter<'de, String, toml::Value>,
    parent_path: String,
    current_entry: Option<&'de toml::Value>,
    last_visited_path: Option<String>,
}

impl<'de> TomlTableAccess<'de> {
    fn new(entries: &'de toml::Table, parent_path: String) -> Self {
        Self {
            iter: entries.iter(),
            parent_path,
            current_entry: None,
            last_visited_path: None,
        }
    }
}

impl<'de> MapAccess<'de> for TomlTableAccess<'de> {
    type Error = DeserError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        let Some((entry_key, entry_val)) = self.iter.next() else {
            return Ok(None);
        };
        let full_path = join_path(&self.parent_path, entry_key);
        self.current_entry = Some(entry_val);
        self.last_visited_path = Some(full_path);
        let mut key_outcome = seed.deserialize(KeyDeserializer(entry_key.as_str()));
        if let Err(ref mut error) = key_outcome {
            error.enrich_key(&self.parent_path, None);
        }
        key_outcome.map(Some)
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let Some(entry_val) = self.current_entry.take() else {
            return Err(de::Error::custom("next_value_seed called without key"));
        };
        let visited_path = self.last_visited_path.clone().unwrap_or_default();
        let mut outcome = seed.deserialize(NodeDeserializer {
            node: NodeRef::Toml(entry_val),
            path: visited_path.clone(),
        });
        if let Err(ref mut error) = outcome {
            error.enrich_value(&visited_path, None);
        }
        outcome
    }
}

struct KeyDeserializer<'de>(&'de str);

impl<'de> Deserializer<'de> for KeyDeserializer<'de> {
    type Error = DeserError;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_borrowed_str(self.0)
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }
}

struct TomlSeqAccess<'de> {
    iter: slice::Iter<'de, toml::Value>,
    path: String,
    index: usize,
}

impl<'de> TomlSeqAccess<'de> {
    fn new(elements: &'de [toml::Value], path: &str) -> Self {
        Self {
            iter: elements.iter(),
            path: path.to_owned(),
            index: 0,
        }
    }
}

impl<'de> SeqAccess<'de> for TomlSeqAccess<'de> {
    type Error = DeserError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        let Some(element) = self.iter.next() else {
            return Ok(None);
        };
        let element_path = format!("{}[{}]", self.path, self.index);
        self.index += 1;
        seed.deserialize(NodeDeserializer {
            node: NodeRef::Toml(element),
            path: element_path,
        })
        .map(Some)
    }
}

struct DefaultSeqAccess<'de> {
    iter: slice::Iter<'de, String>,
    path: String,
    index: usize,
}

impl<'de> DefaultSeqAccess<'de> {
    fn new(elements: &'de [String], path: &str) -> Self {
        Self {
            iter: elements.iter(),
            path: path.to_owned(),
            index: 0,
        }
    }
}

impl<'de> SeqAccess<'de> for DefaultSeqAccess<'de> {
    type Error = DeserError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        let Some(element) = self.iter.next() else {
            return Ok(None);
        };
        let element_path = format!("{}[{}]", self.path, self.index);
        self.index += 1;
        seed.deserialize(NodeDeserializer {
            node: NodeRef::DefaultStr(element.as_str()),
            path: element_path,
        })
        .map(Some)
    }
}

struct EnvSeqAccess<'de> {
    origin: &'de EnvOrigin,
    segments: Option<std::str::Split<'de, char>>,
    path: String,
    index: usize,
}

impl<'de> EnvSeqAccess<'de> {
    fn new(origin: &'de EnvOrigin, raw: &'de str, path: &str) -> Self {
        let segments = (!raw.is_empty()).then(|| raw.split(','));
        Self {
            origin,
            segments,
            path: path.to_owned(),
            index: 0,
        }
    }
}

impl<'de> SeqAccess<'de> for EnvSeqAccess<'de> {
    type Error = DeserError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        let Some(ref mut iter) = self.segments else {
            return Ok(None);
        };
        let Some(segment) = iter.next() else {
            return Ok(None);
        };
        let element_path = format!("{}[{}]", self.path, self.index);
        self.index += 1;
        seed.deserialize(NodeDeserializer {
            node: NodeRef::EnvSegment {
                origin: self.origin,
                segment: segment.trim(),
            },
            path: element_path,
        })
        .map(Some)
    }
}

struct EnumStringAccess<'de> {
    text: &'de str,
    origin: Option<&'de EnvOrigin>,
    variants: &'static [&'static str],
}

impl<'de> EnumStringAccess<'de> {
    const fn new(
        text: &'de str,
        origin: Option<&'de EnvOrigin>,
        variants: &'static [&'static str],
    ) -> Self {
        Self {
            text,
            origin,
            variants,
        }
    }
}

impl<'de> EnumAccess<'de> for EnumStringAccess<'de> {
    type Error = DeserError;
    type Variant = UnitOnlyVariantAccess;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let parsed = seed.deserialize(KeyDeserializer(self.text)).map_err(|_| {
            let expected = format_expected_fields(self.variants);
            self.origin.map_or_else(
                || {
                    DeserError::message(
                        None,
                        None,
                        format!("unknown variant `{}`, {expected}", self.text),
                    )
                },
                |origin| DeserError::invalid_env(origin, format!("invalid variant; {expected}")),
            )
        })?;
        Ok((parsed, UnitOnlyVariantAccess))
    }
}

struct UnitOnlyVariantAccess;

impl<'de> VariantAccess<'de> for UnitOnlyVariantAccess {
    type Error = DeserError;

    fn unit_variant(self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(
        self,
        _seed: T,
    ) -> Result<T::Value, Self::Error> {
        Err(de::Error::custom("unexpected newtype enum variant"))
    }

    fn tuple_variant<V: Visitor<'de>>(
        self,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(de::Error::custom("unexpected tuple enum variant"))
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error> {
        Err(de::Error::custom("unexpected struct enum variant"))
    }
}
