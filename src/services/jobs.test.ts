import { describe, expect, it } from "vitest";

import { extractWorkflowID } from "./jobs";

describe("extractWorkflowID", () => {
  it("returns the canonical river:workflow_id when present", () => {
    expect(extractWorkflowID({ "river:workflow_id": "wf-canonical" })).toBe(
      "wf-canonical",
    );
  });

  it("falls back to the legacy unprefixed workflow_id key", () => {
    expect(extractWorkflowID({ workflow_id: "wf-legacy" })).toBe("wf-legacy");
  });

  it("prefers the canonical key when both are present", () => {
    expect(
      extractWorkflowID({
        "river:workflow_id": "wf-canonical",
        workflow_id: "wf-legacy",
      }),
    ).toBe("wf-canonical");
  });

  it("returns undefined for metadata without workflow keys", () => {
    expect(extractWorkflowID({})).toBeUndefined();
    expect(extractWorkflowID({ other: "value" })).toBeUndefined();
  });

  it("returns undefined for null/undefined input", () => {
    expect(extractWorkflowID(undefined)).toBeUndefined();
    expect(extractWorkflowID(null as unknown as object)).toBeUndefined();
  });

  it("returns undefined when the workflow_id is an empty string", () => {
    expect(
      extractWorkflowID({ "river:workflow_id": "", workflow_id: "" }),
    ).toBeUndefined();
  });

  it("falls through to legacy when canonical is non-string", () => {
    expect(
      extractWorkflowID({
        "river:workflow_id": 123 as unknown as string,
        workflow_id: "wf-legacy",
      }),
    ).toBe("wf-legacy");
  });
});
