import WorkflowDetail from "@components/WorkflowDetail";
import { useRefreshSetting } from "@contexts/RefreshSettings.hook";
import { extractWorkflowID } from "@services/jobs";
import { toastError, toastSuccess } from "@services/toast";
import {
  cancelJobs,
  getWorkflow,
  getWorkflowKey,
  rerunWorkflow,
  retryWorkflow,
  type WorkflowRetryMode,
} from "@services/workflows";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  createFileRoute,
  ErrorComponent,
  useNavigate,
} from "@tanstack/react-router";
import { NotFoundError } from "@utils/api";
import { useMemo } from "react";
import { z } from "zod";

const workflowDetailSearchSchema = z.object({
  selected: z.coerce.bigint().positive().optional(),
});

export const Route = createFileRoute("/workflows/$workflowId")({
  validateSearch: workflowDetailSearchSchema,
  parseParams: ({ workflowId }) => ({ workflowId }),
  stringifyParams: ({ workflowId }) => ({ workflowId }),

  beforeLoad: ({
    abortController,
    context: { features },
    params: { workflowId },
  }) => {
    return {
      queryOptions: {
        enabled: features.workflowQueries,
        queryKey: getWorkflowKey(workflowId),
        queryFn: getWorkflow,
        refetchInterval: 1000,
        signal: abortController.signal,
      },
    };
  },

  loader: async ({ context: { queryClient, queryOptions } }) => {
    if (!queryOptions.enabled) return;
    await queryClient.ensureQueryData(queryOptions);
  },

  errorComponent: ({ error }) => {
    if (error instanceof NotFoundError) {
      return <div>{error.message}</div>;
    }

    return <ErrorComponent error={error} />;
  },

  component: WorkflowComponent,
});

function WorkflowComponent() {
  const { queryOptions } = Route.useRouteContext();
  const { selected: selectedJobId } = Route.useSearch();
  const navigate = useNavigate({ from: Route.fullPath });
  const refreshSettings = useRefreshSetting();
  const queryClient = useQueryClient();
  const queryOptionsWithRefresh = useMemo(
    () => ({ ...queryOptions, refetchInterval: refreshSettings.intervalMs }),
    [queryOptions, refreshSettings.intervalMs],
  );

  const workflowQuery = useQuery(queryOptionsWithRefresh);

  const { data: workflow } = workflowQuery;
  const setSelectedJobId = (jobId: bigint | undefined) => {
    navigate({
      replace: true,
      search: (prev) => ({ ...prev, selected: jobId }),
    });
  };

  const workflowID = extractWorkflowID(workflow?.tasks?.[0]?.metadata);

  // Guard against missing workflow ID with a loud failure so a bad metadata
  // contract surfaces immediately instead of silently swallowing the click.
  const requireWorkflowID = (action: string): string | undefined => {
    if (workflowID) return workflowID;
    const msg = `Cannot ${action}: workflow ID missing from task metadata`;
    console.error(msg, { metadata: workflow?.tasks?.[0]?.metadata });
    toastError({ message: msg, duration: 4000 });
    return undefined;
  };
  const cancelMutation = useMutation({
    mutationFn: cancelJobs,
    onSuccess: () => {
      if (workflowID) {
        queryClient.invalidateQueries({ queryKey: getWorkflowKey(workflowID) });
      }
      queryClient.invalidateQueries({ queryKey: ["listWorkflows"] });
      toastSuccess({
        message: "Workflow cancellation requested",
        duration: 2000,
      });
    },
  });

  const retryMutation = useMutation({
    mutationFn: retryWorkflow,
    onSuccess: () => {
      if (workflowID) {
        queryClient.invalidateQueries({ queryKey: getWorkflowKey(workflowID) });
      }
      queryClient.invalidateQueries({ queryKey: ["listWorkflows"] });
      toastSuccess({
        message: "Workflow retry requested",
        duration: 2000,
      });
    },
  });

  const rerunMutation = useMutation({
    mutationFn: rerunWorkflow,
    onSuccess: (result) => {
      queryClient.invalidateQueries({ queryKey: ["listWorkflows"] });
      toastSuccess({
        message: `New workflow ${result.workflowID} created`,
        duration: 3000,
      });
      navigate({
        to: "/workflows/$workflowId",
        params: { workflowId: result.workflowID },
      });
    },
  });

  return (
    <WorkflowDetail
      cancelPending={cancelMutation.isPending}
      loading={workflowQuery.isLoading}
      onCancel={() => {
        const id = requireWorkflowID("cancel workflow");
        if (!id) return;
        cancelMutation.mutate({ workflowID: id });
      }}
      onRerun={() => {
        const id = requireWorkflowID("rerun workflow");
        if (!id) return;
        rerunMutation.mutate({ workflowID: id });
      }}
      onRetry={(mode: WorkflowRetryMode, resetHistory: boolean) => {
        const id = requireWorkflowID("retry workflow");
        if (!id) return;
        retryMutation.mutate({ workflowID: id, mode, resetHistory });
      }}
      rerunPending={rerunMutation.isPending}
      retryPending={retryMutation.isPending}
      selectedJobId={selectedJobId}
      setSelectedJobId={setSelectedJobId}
      workflow={workflow}
    />
  );
}
