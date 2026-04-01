import { ApiError } from "@/api-gen";

export function getErrorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    const body = error.body as { message?: string } | string | undefined;
    if (typeof body === "string" && body.trim()) {
      return body;
    }
    if (body && typeof body === "object" && "message" in body && typeof body.message === "string") {
      return body.message;
    }
    return error.message;
  }

  if (error instanceof Error) {
    return error.message;
  }

  return "Something went wrong.";
}
