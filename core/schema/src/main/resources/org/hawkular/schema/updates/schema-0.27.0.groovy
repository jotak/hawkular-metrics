/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

schemaChange {
  version '8.0'
  author 'jtakvori'
  tags '0.27.x'
  cql """
  CREATE TABLE blob_store (
      tenant_id text,
      key text,
      value text,
      tags map<text, text>,
      PRIMARY KEY ((tenant_id), key)
  ) WITH compaction = { 'class': 'LeveledCompactionStrategy' }
"""
  verify { tableExists(keyspace, 'blob_store') }
}

schemaChange {
  version '8.1'
  author 'jtakvori'
  tags '0.27.x'
  cql """
  CREATE TABLE blob_store_tags_idx (
      tenant_id text,
      tname text,
      tvalue text,
      key text,
      PRIMARY KEY ((tenant_id, tname), tvalue, key)
  ) WITH compaction = { 'class': 'LeveledCompactionStrategy' }
"""
  verify { tableExists(keyspace, 'blob_store_tags_idx') }
}
