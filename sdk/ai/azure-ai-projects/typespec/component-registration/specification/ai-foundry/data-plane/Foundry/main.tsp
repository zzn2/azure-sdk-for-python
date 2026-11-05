import "@typespec/http";

using TypeSpec.Http;

@service(#{ title: "Microsoft Foundry Pipeline component registration" })
@server("{endpoint}", "Microsoft Foundry project endpoint", { endpoint: url })
namespace Azure.AI.Projects;

model ComponentVersionRequest {
  properties: {
    componentSpec: Record<unknown>;
  };
}

model RegisteredComponentVersion {
  id: string;
}

interface PipelineComponentRegistration {
  @route("/components/{name}/versions/{version}")
  @put
  registerCommandComponent(
    @path name: string,
    @path version: string,
    @query("api-version") apiVersion: "2026-01-15-preview",
    @body component: ComponentVersionRequest,
  ): {
    @statusCode statusCode: 200 | 201;
    @body body: RegisteredComponentVersion;
  };
}
