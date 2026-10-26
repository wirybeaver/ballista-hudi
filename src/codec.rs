// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Hudi provider serialization layered over Ballista’s logical codec.

use std::collections::BTreeMap;
use std::sync::Arc;

use ballista_core::serde::BallistaLogicalExtensionCodec;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::{DataFusionError, Result, TableReference};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Extension;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use hudi_datafusion::HudiDataSource;
use prost::Message;

const HUDI_PROVIDER_PREFIX: &[u8] = b"ballista-contrib-hudi-v1\0";

// A sorted map keeps the protobuf recipe deterministic across processes.
#[derive(Clone, PartialEq, Message)]
struct HudiProvider {
    #[prost(string, tag = 1)]
    base_uri: String,
    #[prost(btree_map = "string, string", tag = 2)]
    options: BTreeMap<String, String>,
}

/// Logical codec that handles Hudi providers and delegates to Ballista’s defaults.
///
/// Decoding requires a multi-thread Tokio runtime.
#[derive(Debug, Default)]
pub struct HudiLogicalExtensionCodec {
    default_codec: BallistaLogicalExtensionCodec,
}

impl LogicalExtensionCodec for HudiLogicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[datafusion::logical_expr::LogicalPlan],
        ctx: &TaskContext,
    ) -> Result<Extension> {
        self.default_codec.try_decode(buf, inputs, ctx)
    }

    fn try_encode(&self, node: &Extension, buf: &mut Vec<u8>) -> Result<()> {
        self.default_codec.try_encode(node, buf)
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &TableReference,
        schema: SchemaRef,
        ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>> {
        let Some(payload) = buf.strip_prefix(HUDI_PROVIDER_PREFIX) else {
            return self
                .default_codec
                .try_decode_table_provider(buf, table_ref, schema, ctx);
        };
        let provider = HudiProvider::decode(payload).map_err(|error| {
            DataFusionError::Internal(format!("failed to decode Hudi provider: {error}"))
        })?;
        let provider = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(HudiDataSource::new_with_options(
                &provider.base_uri,
                provider.options,
            ))
        })?;
        Ok(Arc::new(provider))
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        let Some(provider) = node.downcast_ref::<HudiDataSource>() else {
            return self
                .default_codec
                .try_encode_table_provider(table_ref, node, buf);
        };
        let provider = HudiProvider {
            base_uri: provider.base_uri(),
            options: provider
                .options()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        };
        buf.extend_from_slice(HUDI_PROVIDER_PREFIX);
        provider.encode(buf).map_err(|error| {
            DataFusionError::Internal(format!("failed to encode Hudi provider: {error}"))
        })
    }

    fn try_decode_file_format(
        &self,
        buf: &[u8],
        ctx: &TaskContext,
    ) -> Result<Arc<dyn datafusion::datasource::file_format::FileFormatFactory>> {
        self.default_codec.try_decode_file_format(buf, ctx)
    }

    fn try_encode_file_format(
        &self,
        buf: &mut Vec<u8>,
        node: Arc<dyn datafusion::datasource::file_format::FileFormatFactory>,
    ) -> Result<()> {
        self.default_codec.try_encode_file_format(buf, node)
    }
}
