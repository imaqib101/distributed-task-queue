import { describe, it, expect } from "vitest";
import * as api from "../src/index";

describe("public API", () => {
  it("exports every runtime entry point", () => {
    expect(api.TaskQueue).toBeTypeOf("function");
    expect(api.DeadLetterQueue).toBeTypeOf("function");
    expect(api.categorizeError).toBeTypeOf("function");
    expect(api.DLQMonitor).toBeTypeOf("function");
    expect(api.WorkerPoolStatsTracker).toBeTypeOf("function");
  });
});
