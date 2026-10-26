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

use std::sync::Arc;

use ballista_hudi::HudiLogicalExtensionCodec;
use datafusion::catalog::TableProvider;
use datafusion::common::TableReference;
use datafusion::prelude::SessionContext;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use hudi_datafusion::HudiDataSource;
use hudi_test::SampleTable;

#[tokio::test(flavor = "multi_thread")]
async fn reconstructs_hudi_location_options_and_schema() {
    let table = SampleTable::V6Nonpartitioned.url_to_cow();
    let original = Arc::new(
        HudiDataSource::new_with_options(
            table.as_str(),
            [
                ("hoodie.read.input.partitions", "2"),
                ("hoodie.read.query.type", "snapshot"),
            ],
        )
        .await
        .unwrap(),
    );
    let codec = HudiLogicalExtensionCodec::default();
    let table_ref = TableReference::bare("users");
    let mut bytes = Vec::new();
    codec
        .try_encode_table_provider(&table_ref, original.clone(), &mut bytes)
        .unwrap();
    let restored = codec
        .try_decode_table_provider(
            &bytes,
            &table_ref,
            original.schema(),
            &SessionContext::new().task_ctx(),
        )
        .unwrap();
    let restored = restored.downcast_ref::<HudiDataSource>().unwrap();
    assert_eq!(restored.base_uri(), original.base_uri());
    assert_eq!(restored.options(), original.options());
    assert_eq!(restored.schema(), original.schema());
}
