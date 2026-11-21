import { afterEach, describe, expect, it, vi } from "vitest";

const { mockExecute, mockGet } = vi.hoisted(() => {
  const mockExecute = vi.fn(async () => {});
  const mockGet = vi.fn((cmd) => (cmd === "status" ? { execute: mockExecute } : undefined));
  return { mockExecute, mockGet };
});

vi.mock("../src/commands/index.js", () => ({
  CommandRegistry: {
    get: mockGet,
  },
}));

vi.mock("../src/storage.js", () => ({
  createStorage: vi.fn(() => ({})),
}));

import { printHelp, runCli } from "../src/cli.js";

const testConfig = {
  logLevel: "silent",
  storage: { provider: "filesystem", localPath: "/tmp/mv-cli-test", prefix: "" },
  mongo: { uri: "mongodb://localhost:27017", database: "app" },
  notifications: { onSuccess: false },
};

describe("CLI parser and runner (cli.ts)", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    mockExecute.mockClear();
  });

  it("prints help for empty args, --help, -h, help", async () => {
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    printHelp();
    await runCli(testConfig, []);
    await runCli(testConfig, ["help"]);
    await runCli(testConfig, ["status", "--help"]);
    expect(logSpy).toHaveBeenCalledTimes(4);
  });

  it("generates a base64 key on key:generate", async () => {
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    await runCli(null, ["key:generate"]);
    expect(logSpy).toHaveBeenCalledTimes(1);
    const key = logSpy.mock.calls[0][0];
    expect(Buffer.from(key, "base64").length).toBe(32);
  });

  it("exits with code 1 when config is null or command is unknown", async () => {
    const errSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    vi.spyOn(console, "log").mockImplementation(() => {});
    const exitSpy = vi.spyOn(process, "exit").mockImplementation((code) => {
      throw new Error(`EXIT_${code}`);
    });

    await expect(runCli(null, ["status"])).rejects.toThrow("EXIT_1");
    await expect(runCli(testConfig, ["does-not-exist"])).rejects.toThrow("EXIT_1");
    expect(errSpy).toHaveBeenCalledTimes(2);
    expect(exitSpy).toHaveBeenCalledWith(1);
  });

  it("executes a valid registered command with parsed flags", async () => {
    await runCli(testConfig, ["status", "--json", "--key", "latest", "--drop", "--force"]);
    expect(mockExecute).toHaveBeenCalledWith(
      expect.objectContaining({
        config: testConfig,
        values: expect.objectContaining({
          json: true,
          key: "latest",
          drop: true,
          force: true,
        }),
      }),
    );
  });
});
