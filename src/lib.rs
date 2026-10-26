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

#![doc = include_str!("../README.md")]
#![warn(missing_docs)]

mod codec;

use std::sync::Arc;

use ballista_core::extension::SessionConfigExt;
use datafusion::common::Result;
use datafusion::prelude::{SessionConfig, SessionContext};
use hudi_datafusion::HudiDataSource;

pub use codec::HudiLogicalExtensionCodec;

/// Installs the Hudi logical codec, retaining Ballista’s default behavior.
///
/// This replaces any previously configured logical extension codec.
/// Provider decoding requires a multi-thread Tokio runtime.
pub fn register_hudi_codec(config: SessionConfig) -> SessionConfig {
    config.with_ballista_logical_extension_codec(Arc::new(HudiLogicalExtensionCodec::default()))
}

/// Creates and registers a Hudi table using Hudi and storage options.
pub async fn register_hudi_table<I, K, V>(
    ctx: &SessionContext,
    name: &str,
    base_uri: &str,
    options: I,
) -> Result<()>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    let provider = HudiDataSource::new_with_options(base_uri, options).await?;
    ctx.register_table(name, Arc::new(provider))?;
    Ok(())
}
