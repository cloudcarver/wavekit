"use client";

import { useMemo } from "react";

import { sql } from "@codemirror/lang-sql";
import { StreamLanguage } from "@codemirror/language";
import { shell } from "@codemirror/legacy-modes/mode/shell";
import CodeMirror from "@uiw/react-codemirror";

import { NotebookCellType } from "@/api-gen";
import { cn } from "@/lib/utils";

type CodeEditorProps = {
  value: string;
  cellType: NotebookCellType;
  onChange: (value: string) => void;
  className?: string;
};

const shellLanguage = StreamLanguage.define(shell);

export function CodeEditor({ value, cellType, onChange, className }: CodeEditorProps) {
  const extensions = useMemo(
    () => [cellType === NotebookCellType.SQL ? sql() : shellLanguage],
    [cellType],
  );

  return (
    <div className={cn("overflow-hidden rounded-md border border-border/80 bg-[#fbfbfd]", className)}>
      <CodeMirror
        basicSetup={{
          foldGutter: false,
          highlightActiveLine: true,
          highlightActiveLineGutter: false,
          lineNumbers: true,
        }}
        className="wavekit-code-editor"
        extensions={extensions}
        height="220px"
        onChange={onChange}
        value={value}
      />
    </div>
  );
}
