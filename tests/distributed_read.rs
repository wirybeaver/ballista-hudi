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

use ballista::datafusion::arrow::array::{Int32Array, StringArray};
use ballista::datafusion::execution::SessionStateBuilder;
use ballista::datafusion::prelude::{SessionConfig, SessionContext};
use ballista::prelude::{SessionConfigExt, SessionContextExt};
use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
use ballista_hudi::{register_hudi_codec, register_hudi_table};
use hudi_test::SampleTable;

#[tokio::test(flavor = "multi_thread")]
async fn reads_projected_hudi_cow_rows_through_ballista() {
    let config = register_hudi_codec(SessionConfig::new_with_ballista().with_target_partitions(2));
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();
    let addr = ballista_scheduler::standalone::new_standalone_scheduler_from_state(&state)
        .await
        .unwrap();
    let scheduler_url = format!("http://localhost:{}", addr.port());
    let scheduler = SchedulerGrpcClient::connect(scheduler_url.clone())
        .await
        .unwrap();
    // Two independent in-process executors, each with one task slot.
    for _ in 0..2 {
        ballista_executor::new_standalone_executor_from_state(scheduler.clone(), 1, &state)
            .await
            .unwrap();
    }
    let ctx = SessionContext::remote_with_state(&scheduler_url, state)
        .await
        .unwrap();
    let table = SampleTable::V6Nonpartitioned.url_to_cow();
    register_hudi_table(
        &ctx,
        "users",
        table.as_str(),
        [("hoodie.read.input.partitions", "2")],
    )
    .await
    .unwrap();

    let batches = ctx
        .sql("SELECT id, name FROM users WHERE \"isActive\" ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let rows = batches
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let names = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..batch.num_rows())
                .map(|row| (ids.value(row), names.value(row).to_owned()))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(rows, vec![(3, "Carol".to_owned()), (4, "Diana".to_owned())]);
}
