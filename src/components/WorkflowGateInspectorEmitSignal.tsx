import { toastError, toastSuccess } from "@services/toast";
import {
  emitWorkflowTaskSignal,
  getWorkflowKey,
  type WorkflowTaskSignal,
} from "@services/workflows";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";

import { defaultPayloadFromCel } from "./emitSignalPayload";

export type EmitSignalFn = typeof emitWorkflowTaskSignal;

type EmitSignalFormProps = {
  celExpr?: string;
  emit?: EmitSignalFn;
  onCancel: () => void;
  onSuccess: (signal: WorkflowTaskSignal) => void;
  signalKey: string;
  workflowID: string;
};

const EmitSignalForm = ({
  celExpr,
  emit = emitWorkflowTaskSignal,
  onCancel,
  onSuccess,
  signalKey,
  workflowID,
}: EmitSignalFormProps) => {
  const [payloadText, setPayloadText] = useState(() =>
    defaultPayloadFromCel(celExpr),
  );
  const [parseError, setParseError] = useState<string>();

  const mutation = useMutation<
    WorkflowTaskSignal,
    Error,
    { key: string; payload: unknown; workflowID: string }
  >({
    mutationFn: emit,
    onError: (error) => {
      toastError({
        duration: 4000,
        message: "Failed to emit signal",
        subtext: error.message,
      });
    },
    onSuccess: (signal) => {
      toastSuccess({ duration: 2000, message: "Signal emitted" });
      onSuccess(signal);
    },
  });

  const handleSubmit = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setParseError(undefined);

    let parsed: unknown;
    try {
      parsed = JSON.parse(payloadText) as unknown;
    } catch {
      setParseError("Invalid JSON — please fix the payload before submitting.");
      return;
    }

    mutation.mutate({ key: signalKey, payload: parsed, workflowID });
  };

  return (
    <form
      className="mt-3 md:ml-[calc(5.75rem+0.75rem)]"
      onSubmit={handleSubmit}
    >
      <div className="rounded-lg border border-slate-200 bg-white p-3 dark:border-slate-800 dark:bg-slate-950/20">
        <div className="space-y-2">
          <div>
            <label
              className="block text-xs font-medium text-slate-600 dark:text-slate-300"
              htmlFor={`emit-signal-key-${signalKey}`}
            >
              Key
            </label>
            <input
              className="mt-1 w-full rounded border border-slate-200 bg-slate-50 px-2 py-1 font-mono text-xs text-slate-800 dark:border-slate-700 dark:bg-slate-900/60 dark:text-slate-200"
              id={`emit-signal-key-${signalKey}`}
              readOnly
              type="text"
              value={signalKey}
            />
          </div>

          <div>
            <label
              className="block text-xs font-medium text-slate-600 dark:text-slate-300"
              htmlFor={`emit-signal-payload-${signalKey}`}
            >
              Payload
            </label>
            <textarea
              className="mt-1 w-full rounded border border-slate-200 bg-slate-50 px-2 py-1 font-mono text-xs text-slate-800 dark:border-slate-700 dark:bg-slate-900/60 dark:text-slate-200"
              id={`emit-signal-payload-${signalKey}`}
              onChange={(event) => {
                setPayloadText(event.target.value);
                setParseError(undefined);
              }}
              rows={4}
              value={payloadText}
            />
            {parseError ? (
              <p
                className="mt-1 text-xs text-red-600 dark:text-red-400"
                role="alert"
              >
                {parseError}
              </p>
            ) : null}
          </div>
        </div>

        <div className="mt-3 flex gap-2">
          <button
            className="inline-flex items-center rounded-md border border-brand-primary bg-brand-primary px-2.5 py-1 text-xs font-medium text-white hover:opacity-90 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-brand-primary disabled:cursor-not-allowed disabled:opacity-50"
            disabled={mutation.isPending}
            type="submit"
          >
            {mutation.isPending ? "Emitting…" : "Emit"}
          </button>
          <button
            className="inline-flex items-center rounded-md border border-slate-300 px-2.5 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-300 dark:hover:bg-slate-800"
            onClick={onCancel}
            type="button"
          >
            Cancel
          </button>
        </div>
      </div>
    </form>
  );
};

type EmitSignalButtonProps = {
  celExpr?: string;
  emit?: EmitSignalFn;
  signalKey: string;
  workflowID: string;
};

export const EmitSignalButton = ({
  celExpr,
  emit,
  signalKey,
  workflowID,
}: EmitSignalButtonProps) => {
  const [formOpen, setFormOpen] = useState(false);
  const queryClient = useQueryClient();

  const handleSuccess = () => {
    setFormOpen(false);
    queryClient.invalidateQueries({ queryKey: getWorkflowKey(workflowID) });
    queryClient.invalidateQueries({ queryKey: ["listWorkflows"] });
  };

  if (formOpen) {
    return (
      <EmitSignalForm
        celExpr={celExpr}
        emit={emit}
        onCancel={() => setFormOpen(false)}
        onSuccess={handleSuccess}
        signalKey={signalKey}
        workflowID={workflowID}
      />
    );
  }

  return (
    <div className="mt-3 md:ml-[calc(5.75rem+0.75rem)]">
      <button
        className="inline-flex items-center rounded-md border border-slate-300 px-2.5 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-300 dark:hover:bg-slate-800"
        onClick={() => setFormOpen(true)}
        type="button"
      >
        Emit signal
      </button>
    </div>
  );
};
