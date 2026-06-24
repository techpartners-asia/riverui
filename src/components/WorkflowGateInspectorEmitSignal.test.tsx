import { type WorkflowTaskSignal } from "@services/workflows";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { type ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  EmitSignalButton,
  type EmitSignalFn,
} from "./WorkflowGateInspectorEmitSignal";

describe("EmitSignalButton", () => {
  beforeEach(() => {
    document.body.innerHTML =
      '<script id="config__json">{"apiUrl":"http://example.test/api"}</script>';
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  const makeWrapper = () => {
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });
    const Wrapper = ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );
    return Wrapper;
  };

  const makeEmitFn = (impl?: () => Promise<WorkflowTaskSignal>): EmitSignalFn =>
    vi.fn<EmitSignalFn>(impl);

  const renderButton = (emitFn: EmitSignalFn) =>
    render(
      <EmitSignalButton
        emit={emitFn}
        signalKey="approval.received"
        workflowID="wf-123"
      />,
      { wrapper: makeWrapper() },
    );

  it("shows 'Emit signal' button, clicking reveals inline form with key prefilled", async () => {
    const emitFn = makeEmitFn();
    renderButton(emitFn);

    expect(
      screen.getByRole("button", { name: "Emit signal" }),
    ).toBeInTheDocument();

    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: "Emit signal" }));
    });

    expect(screen.getByRole("button", { name: "Emit" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Cancel" })).toBeInTheDocument();

    const keyInput = screen.getByLabelText("Key");
    expect(keyInput).toHaveValue("approval.received");
    expect(keyInput).toHaveAttribute("readonly");

    const payloadTextarea = screen.getByLabelText("Payload");
    expect(payloadTextarea).toHaveValue('{\n  "approved": true\n}');
  });

  it("calls emit with workflowID, key, and parsed payload on valid submit", async () => {
    const resolved: WorkflowTaskSignal = {
      attempt: 1,
      createdAt: new Date("2026-06-24T00:00:00Z"),
      id: 42n,
      key: "approval.received",
      payload: { approved: true },
      source: null,
    };
    const emitFn = makeEmitFn(async () => resolved);
    renderButton(emitFn);

    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: "Emit signal" }));
    });

    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: "Emit" }));
    });

    await waitFor(() => expect(emitFn).toHaveBeenCalledTimes(1));
    // useMutation passes (variables, { client, meta, mutationKey }) — match first call's first arg:
    expect(vi.mocked(emitFn).mock.calls[0]?.[0]).toEqual({
      key: "approval.received",
      payload: { approved: true },
      workflowID: "wf-123",
    });
  });

  it("blocks submit and shows error when payload is invalid JSON", async () => {
    const emitFn = makeEmitFn();
    renderButton(emitFn);

    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: "Emit signal" }));
    });

    const payloadTextarea = screen.getByLabelText("Payload");
    await act(async () => {
      fireEvent.change(payloadTextarea, {
        target: { value: "not valid json" },
      });
    });

    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: "Emit" }));
    });

    expect(emitFn).not.toHaveBeenCalled();
    expect(screen.getByRole("alert")).toHaveTextContent(
      "Invalid JSON — please fix the payload before submitting.",
    );
  });

  it("Cancel button closes the form and shows 'Emit signal' again", async () => {
    const emitFn = makeEmitFn();
    renderButton(emitFn);

    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: "Emit signal" }));
    });

    expect(screen.getByRole("button", { name: "Cancel" })).toBeInTheDocument();

    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    });

    expect(
      screen.getByRole("button", { name: "Emit signal" }),
    ).toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "Emit" }),
    ).not.toBeInTheDocument();
  });
});
