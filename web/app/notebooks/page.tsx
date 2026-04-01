import { Suspense } from "react";

import { NotebookPageClient } from "./_components/notebook-page-client";

function NotebookPageFallback() {
  return (
    <main className="min-h-screen bg-background px-3 py-4 md:px-4 lg:px-5">
      <div className="mx-auto max-w-5xl">Loading notebook…</div>
    </main>
  );
}

export default function NotebookPage() {
  return (
    <Suspense fallback={<NotebookPageFallback />}>
      <NotebookPageClient />
    </Suspense>
  );
}
