/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package main

import (
	"log"

	"github.com/GoogleCloudPlatform/DataflowTemplates/cicd/internal/workflows"
)

func main() {
	mvnFlags := workflows.NewMavenFlags()
	err := workflows.MvnVerify().Run(
		mvnFlags.IncludeDependencies(),
		mvnFlags.IncludeDependents(),
		mvnFlags.SkipCheckstyle(),
		mvnFlags.SetFlag("-Dtest", "MongoDbToBigQueryIT"),
		mvnFlags.SetFlag("-Dproject", "jeff-test-project-templates"),
		mvnFlags.SetFlag("-Dregion", "us-central1"),
		mvnFlags.SetFlag("-DspecPath", "gs://dataflow-templates/latest/flex/MongoDB_to_BigQuery"),
		mvnFlags.SetFlag("-DhostIp", "$(hostname -I | awk '{print $1}')"),
		mvnFlags.SkipDependencyAnalysis(),
		mvnFlags.SkipJib(),
		mvnFlags.FailAtTheEnd())
	if err != nil {
		log.Fatalf("%v\n", err)
	}
	log.Println("Build Successful!")
}
