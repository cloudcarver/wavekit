"use client";

import Link from "next/link";
import { useSearchParams } from "next/navigation";

import { NotebookWorkspace } from "@/app/_components/notebook-workspace";
import { buttonVariants } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

export function NotebookPageClient() {
  const searchParams = useSearchParams();
  const notebookId = searchParams.get("id")?.trim();

  if (!notebookId) {
    return (
      <main className="min-h-screen bg-background px-3 py-4 md:px-4 lg:px-5">
        <div className="mx-auto max-w-3xl">
          <Card className="border-border bg-card">
            <CardHeader>
              <CardTitle>No notebook selected</CardTitle>
              <CardDescription>
                Choose a notebook from the dashboard to open the workspace.
              </CardDescription>
            </CardHeader>
            <CardContent>
              <Link className={buttonVariants({ variant: "secondary" })} href="/">
                Back to dashboard
              </Link>
            </CardContent>
          </Card>
        </div>
      </main>
    );
  }

  return <NotebookWorkspace notebookId={notebookId} />;
}
