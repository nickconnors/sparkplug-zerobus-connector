export class AuthRequiredError extends Error {
  readonly name = "AuthRequiredError";
}

function isJsonContentType(ct: string | null): boolean {
  // Accept `application/json` plus vendor types like `application/problem+json`.
  return (ct ?? "").toLowerCase().includes("json");
}

async function parseBody(resp: Response): Promise<unknown> {
  const ct = resp.headers.get("content-type");
  if (isJsonContentType(ct)) return await resp.json();
  return await resp.text();
}

export async function apiFetch(path: string, options: RequestInit = {}): Promise<unknown> {
  const resp = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(options.headers ?? {}) },
    credentials: "include", // cookie-based auth (the login's session cookie)
    ...options,
  });

  if (resp.status === 401) {
    // signed out (or the session expired): to the login page, once, instead of an error per request
    if (!window.location.pathname.startsWith("/static/login.html")) window.location.assign("/static/login.html");
    throw new AuthRequiredError("Authentication required");
  }

  const body = await parseBody(resp);

  if (!resp.ok) {
    const msg =
      typeof body === "string"
        ? body
        : typeof (body as any)?.message === "string"
          ? (body as any).message
          : `HTTP ${resp.status}`;
    throw new Error(msg);
  }

  return body;
}

export async function apiJson<T>(path: string, options?: RequestInit): Promise<T> {
  return (await apiFetch(path, options)) as T;
}

export async function apiText(path: string, options?: RequestInit): Promise<string> {
  const body = await apiFetch(path, options);
  return typeof body === "string" ? body : JSON.stringify(body, null, 2);
}

