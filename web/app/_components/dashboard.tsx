"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";

import { DefaultService, type Cluster, type NotebookSummary } from "@/api-gen";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { getErrorMessage } from "@/lib/api";
import { cn } from "@/lib/utils";

type ClusterFormState = {
  clusterName: string;
  sqlConnectionString: string;
  metaNodeGrpcUrl: string;
  metaNodeHttpUrl: string;
};

type ClusterUpdateFormState = ClusterFormState;

const emptyClusterForm: ClusterFormState = {
  clusterName: "",
  sqlConnectionString: "",
  metaNodeGrpcUrl: "",
  metaNodeHttpUrl: "",
};

export function Dashboard() {
  const router = useRouter();
  const [clusters, setClusters] = useState<Cluster[]>([]);
  const [notebooks, setNotebooks] = useState<NotebookSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [clusterForm, setClusterForm] = useState<ClusterFormState>(emptyClusterForm);
  const [notebookName, setNotebookName] = useState("");
  const [editingClusterId, setEditingClusterId] = useState<string | null>(null);
  const [editingClusterForm, setEditingClusterForm] = useState<ClusterUpdateFormState | null>(null);
  const [connectDialogOpen, setConnectDialogOpen] = useState(false);
  const [createNotebookDialogOpen, setCreateNotebookDialogOpen] = useState(false);

  const refresh = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [clusterResponse, notebookResponse] = await Promise.all([
        DefaultService.listClusters(),
        DefaultService.listNotebooks(),
      ]);
      setClusters(clusterResponse.clusters);
      setNotebooks(notebookResponse.notebooks);
    } catch (refreshError) {
      setError(getErrorMessage(refreshError));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  async function handleConnectCluster(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setBusy(true);
    setError(null);
    try {
      await DefaultService.connectCluster(toClusterRequest(clusterForm));
      setClusterForm(emptyClusterForm);
      setConnectDialogOpen(false);
      await refresh();
    } catch (connectError) {
      setError(getErrorMessage(connectError));
    } finally {
      setBusy(false);
    }
  }

  async function handleCreateNotebook(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setBusy(true);
    setError(null);
    try {
      await DefaultService.createNotebook({ notebookName });
      setNotebookName("");
      setCreateNotebookDialogOpen(false);
      await refresh();
    } catch (createError) {
      setError(getErrorMessage(createError));
    } finally {
      setBusy(false);
    }
  }

  async function handleDeleteCluster(clusterUuid: string) {
    setBusy(true);
    setError(null);
    try {
      await DefaultService.deleteCluster(clusterUuid);
      if (editingClusterId === clusterUuid) {
        setEditingClusterId(null);
        setEditingClusterForm(null);
      }
      await refresh();
    } catch (deleteError) {
      setError(getErrorMessage(deleteError));
    } finally {
      setBusy(false);
    }
  }

  async function handleDeleteNotebook(notebookUuid: string) {
    setBusy(true);
    setError(null);
    try {
      await DefaultService.deleteNotebook(notebookUuid);
      await refresh();
    } catch (deleteError) {
      setError(getErrorMessage(deleteError));
    } finally {
      setBusy(false);
    }
  }

  function beginEditCluster(cluster: Cluster) {
    setEditingClusterId(cluster.clusterUuid);
    setEditingClusterForm({
      clusterName: cluster.clusterName,
      sqlConnectionString: cluster.sqlConnectionString,
      metaNodeGrpcUrl: cluster.metaNodeGrpcUrl,
      metaNodeHttpUrl: cluster.metaNodeHttpUrl,
    });
  }

  async function handleSaveCluster(clusterUuid: string) {
    if (!editingClusterForm) {
      return;
    }

    setBusy(true);
    setError(null);
    try {
      await DefaultService.updateCluster(clusterUuid, toClusterRequest(editingClusterForm));
      setEditingClusterId(null);
      setEditingClusterForm(null);
      await refresh();
    } catch (updateError) {
      setError(getErrorMessage(updateError));
    } finally {
      setBusy(false);
    }
  }

  const summary = useMemo(
    () => ({
      clusterCount: clusters.length,
      notebookCount: notebooks.length,
    }),
    [clusters, notebooks],
  );

  return (
    <>
      <main className="min-h-screen bg-background px-3 py-4 md:px-4 lg:px-5">
        <div className="mx-auto flex max-w-7xl flex-col gap-5">
          <Card className="border-border bg-card">
            <CardContent className="space-y-4 p-4 md:p-5">
              <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_auto] lg:items-end">
                <div className="max-w-2xl space-y-3">
                  <Badge variant="secondary" className="w-fit uppercase tracking-[0.2em]">
                    WaveKit
                  </Badge>
                  <div className="space-y-1.5">
                    <h1 className="text-2xl font-semibold tracking-tight md:text-3xl">
                      WaveKit
                    </h1>
                  </div>
                </div>
                <div className="grid grid-cols-2 gap-2.5 text-xs lg:self-end">
                  <StatCard label="Clusters" value={summary.clusterCount} />
                  <StatCard label="Notebooks" value={summary.notebookCount} />
                </div>
              </div>

              {error ? (
                <Alert variant="destructive">
                  <AlertTitle>Request failed</AlertTitle>
                  <AlertDescription>{error}</AlertDescription>
                </Alert>
              ) : null}
            </CardContent>
          </Card>

          <section className="grid gap-5 lg:grid-cols-[minmax(0,1.2fr)_minmax(320px,0.8fr)]">
            <Card className="border-border bg-card">
              <CardHeader className="flex flex-col gap-3 border-b border-border/70 pb-3 md:flex-row md:items-start md:justify-between">
                <div className="max-w-lg">
                  <CardTitle>Clusters</CardTitle>
                  <CardDescription>
                    Manage saved SQL and meta connection details for your RisingWave clusters.
                  </CardDescription>
                </div>
                <div className="flex flex-wrap gap-2">
                  <Button onClick={() => setConnectDialogOpen(true)} size="sm" variant="secondary">
                    Connect cluster
                  </Button>
                  <Button onClick={() => void refresh()} size="sm" variant="outline">
                    Refresh
                  </Button>
                </div>
              </CardHeader>
              <CardContent className="space-y-4 px-4 pb-4 pt-4">
                <div className="space-y-3">
                  {!loading && clusters.length === 0 ? (
                    <EmptyState
                      description="Add the SQL DSN plus meta node endpoints to start monitoring a RisingWave cluster."
                      title="No clusters yet"
                    />
                  ) : null}
                  {clusters.map((cluster) => {
                    const isEditing = editingClusterId === cluster.clusterUuid && editingClusterForm;
                    return (
                      <Card key={cluster.clusterUuid} className="border-border/80 bg-white shadow-none">
                        <CardContent className="space-y-3 !p-3">
                          <div className="grid gap-3 lg:grid-cols-[minmax(0,1fr)_auto] lg:items-start">
                            <div className="space-y-2">
                              <div className="flex flex-wrap items-center gap-2">
                                <h3 className="text-base font-semibold">{cluster.clusterName}</h3>
                              </div>
                              <dl className="grid gap-2 text-xs text-muted-foreground">
                                <MetadataRow label="Cluster UUID" mono value={cluster.clusterUuid} />
                                <MetadataRow
                                  description={cluster.connectionStatus.sql.message}
                                  label="SQL"
                                  mono
                                  value={cluster.sqlConnectionString}
                                />
                                <MetadataRow
                                  description={cluster.connectionStatus.meta.message}
                                  extraValue={cluster.metaNodeHttpUrl}
                                  label="Meta"
                                  mono
                                  value={cluster.metaNodeGrpcUrl}
                                />
                              </dl>
                            </div>

                            <div className="flex flex-wrap gap-2 lg:justify-end">
                              <Button onClick={() => beginEditCluster(cluster)} size="sm" variant="outline">
                                Edit
                              </Button>
                              <Button onClick={() => void handleDeleteCluster(cluster.clusterUuid)} size="sm" variant="destructive">
                                Delete
                              </Button>
                            </div>
                          </div>

                          {isEditing ? (
                            <div className="grid gap-3 rounded-lg border border-border bg-muted/40 p-3">
                              <div className="grid gap-3 md:grid-cols-2">
                                <LabeledInput
                                  label="Cluster name"
                                  onChange={(value) =>
                                    setEditingClusterForm((current) =>
                                      current ? { ...current, clusterName: value } : current,
                                    )
                                  }
                                  value={editingClusterForm.clusterName}
                                />
                                <LabeledInput
                                  label="Meta gRPC URL (optional)"
                                  onChange={(value) =>
                                    setEditingClusterForm((current) =>
                                      current ? { ...current, metaNodeGrpcUrl: value } : current,
                                    )
                                  }
                                  value={editingClusterForm.metaNodeGrpcUrl}
                                />
                              </div>
                              <LabeledInput
                                label="SQL connection string"
                                onChange={(value) =>
                                  setEditingClusterForm((current) =>
                                    current ? { ...current, sqlConnectionString: value } : current,
                                  )
                                }
                                value={editingClusterForm.sqlConnectionString}
                              />
                              <LabeledInput
                                label="Meta HTTP URL (optional)"
                                onChange={(value) =>
                                  setEditingClusterForm((current) =>
                                    current ? { ...current, metaNodeHttpUrl: value } : current,
                                  )
                                }
                                value={editingClusterForm.metaNodeHttpUrl}
                              />
                              <div className="flex flex-wrap justify-end gap-2">
                                <Button
                                  onClick={() => {
                                    setEditingClusterId(null);
                                    setEditingClusterForm(null);
                                  }}
                                  size="sm"
                                  variant="outline"
                                >
                                  Cancel
                                </Button>
                                <Button onClick={() => void handleSaveCluster(cluster.clusterUuid)} size="sm">
                                  Save cluster
                                </Button>
                              </div>
                            </div>
                          ) : null}
                        </CardContent>
                      </Card>
                    );
                  })}
                </div>
              </CardContent>
            </Card>

            <Card className="border-border bg-card">
              <CardHeader className="flex flex-col gap-3 border-b border-border/70 pb-3 md:flex-row md:items-start md:justify-between">
                <div className="max-w-lg">
                  <CardTitle>SQL notebooks</CardTitle>
                  <CardDescription>
                    Keep saved SQL cells and open each notebook in a focused, three-column workspace.
                  </CardDescription>
                </div>
                <Button onClick={() => setCreateNotebookDialogOpen(true)} size="sm" variant="secondary">
                  Create notebook
                </Button>
              </CardHeader>
              <CardContent className="space-y-4 px-4 pb-4 pt-4">
                <div className="space-y-3">
                  {!loading && notebooks.length === 0 ? (
                    <EmptyState
                      description="Create a notebook to start composing reusable SQL cells with cluster context."
                      title="No notebooks yet"
                    />
                  ) : null}
                  {notebooks.map((notebook) => (
                    <Card key={notebook.notebookUuid} className="border-border/80 bg-white shadow-none">
                      <CardContent className="flex flex-col gap-3 !p-3 sm:flex-row sm:items-center sm:justify-between">
                        <div className="space-y-1 sm:min-w-0 sm:flex-1">
                          <h3 className="text-base font-semibold">{notebook.notebookName}</h3>
                          <p className="text-xs text-muted-foreground">
                            {formatTimestamp(notebook.updatedAt)}
                          </p>
                        </div>
                        <div className="flex flex-wrap gap-2 sm:justify-end">
                          <Button
                            onClick={() => router.push(`/notebooks/?id=${encodeURIComponent(notebook.notebookUuid)}`)}
                            size="sm"
                            variant="secondary"
                          >
                            Open workspace
                          </Button>
                          <Button onClick={() => void handleDeleteNotebook(notebook.notebookUuid)} size="sm" variant="destructive">
                            Delete
                          </Button>
                        </div>
                      </CardContent>
                    </Card>
                  ))}
                </div>
              </CardContent>
            </Card>
          </section>
        </div>
      </main>

      <Dialog onOpenChange={setConnectDialogOpen} open={connectDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Connect cluster</DialogTitle>
            <DialogDescription>
              Add the SQL connection string and optional meta endpoints to start monitoring a RisingWave cluster.
            </DialogDescription>
          </DialogHeader>

          {error ? (
            <Alert className="mb-3" variant="destructive">
              <AlertTitle>Request failed</AlertTitle>
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          ) : null}

          <form className="grid gap-3" onSubmit={handleConnectCluster}>
            <div className="grid gap-3 md:grid-cols-2">
              <LabeledInput
                label="Cluster name"
                onChange={(value) => setClusterForm((current) => ({ ...current, clusterName: value }))}
                placeholder="prod-eu-1"
                value={clusterForm.clusterName}
              />
              <LabeledInput
                label="Meta gRPC URL (optional)"
                onChange={(value) => setClusterForm((current) => ({ ...current, metaNodeGrpcUrl: value }))}
                placeholder="http://127.0.0.1:5690"
                value={clusterForm.metaNodeGrpcUrl}
              />
            </div>
            <LabeledInput
              label="SQL connection string"
              onChange={(value) => setClusterForm((current) => ({ ...current, sqlConnectionString: value }))}
              placeholder="postgres://root:***@127.0.0.1:4566/dev"
              value={clusterForm.sqlConnectionString}
            />
            <LabeledInput
              label="Meta HTTP URL (optional)"
              onChange={(value) => setClusterForm((current) => ({ ...current, metaNodeHttpUrl: value }))}
              placeholder="http://127.0.0.1:5691"
              value={clusterForm.metaNodeHttpUrl}
            />
            <DialogFooter>
              <Button onClick={() => setConnectDialogOpen(false)} size="sm" variant="outline">
                Cancel
              </Button>
              <Button disabled={busy} size="sm" type="submit">
                {busy ? "Working…" : "Connect cluster"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>

      <Dialog onOpenChange={setCreateNotebookDialogOpen} open={createNotebookDialogOpen}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>Create notebook</DialogTitle>
            <DialogDescription>
              Create a notebook to keep reusable SQL cells and open it in the workspace.
            </DialogDescription>
          </DialogHeader>

          {error ? (
            <Alert className="mb-3" variant="destructive">
              <AlertTitle>Request failed</AlertTitle>
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          ) : null}

          <form className="grid gap-3" onSubmit={handleCreateNotebook}>
            <LabeledInput
              label="Notebook name"
              onChange={setNotebookName}
              placeholder="Operational checks"
              value={notebookName}
            />
            <DialogFooter>
              <Button onClick={() => setCreateNotebookDialogOpen(false)} size="sm" variant="outline">
                Cancel
              </Button>
              <Button disabled={busy} size="sm" type="submit">
                Create notebook
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>
    </>
  );
}

function StatCard({ label, value }: { label: string; value: number }) {
  return (
    <Card className="min-w-[88px] border-border bg-card shadow-none">
      <CardContent className="px-3 py-2 text-center">
        <div className="text-lg font-semibold tracking-tight">{value}</div>
        <div className="text-[11px] uppercase tracking-[0.14em] text-muted-foreground">{label}</div>
      </CardContent>
    </Card>
  );
}

function LabeledInput({
  label,
  onChange,
  placeholder,
  value,
}: {
  label: string;
  value: string;
  placeholder?: string;
  onChange: (value: string) => void;
}) {
  return (
    <label className="grid gap-1.5 text-xs">
      <span className="text-[11px] font-medium tracking-[0.01em] text-muted-foreground">{label}</span>
      <Input onChange={(event) => onChange(event.target.value)} placeholder={placeholder} value={value} />
    </label>
  );
}

function EmptyState({ title, description }: { title: string; description: string }) {
  return (
    <div className="rounded-md border border-dashed border-border/80 bg-[#fbfbfd] px-3 py-6 text-center">
      <h3 className="text-sm font-semibold">{title}</h3>
      <p className="mx-auto mt-1.5 max-w-md text-xs leading-5 text-muted-foreground">{description}</p>
    </div>
  );
}

function MetadataRow({
  label,
  value,
  extraValue,
  description,
  mono = false,
}: {
  label: string;
  value: string | null | undefined;
  extraValue?: string | null;
  description?: string;
  mono?: boolean;
}) {
  return (
    <div>
      <dt className="text-[11px] font-medium tracking-[0.01em] text-foreground">{label}</dt>
      <dd className={cn("mt-0.5 break-all text-xs", mono && "font-mono text-[11px]")}>{value || "—"}</dd>
      {extraValue ? <dd className={cn("mt-0.5 break-all text-xs", mono && "font-mono text-[11px]")}>{extraValue}</dd> : null}
      {description ? <dd className="mt-0.5 text-[11px]">{description}</dd> : null}
    </div>
  );
}

function toClusterRequest(form: ClusterFormState) {
  return {
    clusterName: form.clusterName,
    sqlConnectionString: form.sqlConnectionString,
    metaNodeGrpcUrl: emptyToUndefined(form.metaNodeGrpcUrl),
    metaNodeHttpUrl: emptyToUndefined(form.metaNodeHttpUrl),
  };
}

function emptyToUndefined(value: string) {
  const trimmed = value.trim();
  return trimmed === "" ? undefined : trimmed;
}

function formatTimestamp(value: string) {
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(value));
}
