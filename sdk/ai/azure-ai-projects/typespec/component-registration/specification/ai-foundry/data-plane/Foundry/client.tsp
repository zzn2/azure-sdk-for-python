import "./main.tsp";
import "@azure-tools/typespec-client-generator-core";

using Azure.ClientGenerator.Core;

@@clientName(Azure.AI.Projects, "AIProjectClient");
@@access(Azure.AI.Projects.PipelineComponentRegistration.registerCommandComponent, Access.internal, "python");
