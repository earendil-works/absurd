export interface RuntimeConfig {
  basePath: string;
  apiBasePath: string;
  staticBasePath: string;
}

declare global {
  interface Window {
    __HABITAT_RUNTIME_CONFIG__?: Partial<RuntimeConfig>;
  }
}

function normalizePrefix(value: string | undefined | null): string {
  const trimmed = (value ?? "").trim();
  if (trimmed === "" || trimmed === "/") {
    return "";
  }

  const path = trimmed.startsWith("/") ? trimmed : `/${trimmed}`;
  return path.replace(/\/+$/, "");
}

function joinPrefixes(...parts: Array<string | undefined | null>): string {
  let result = "";
  for (const part of parts) {
    const normalized = normalizePrefix(part);
    if (!normalized) {
      continue;
    }
    if (!result) {
      result = normalized;
      continue;
    }
    result += normalized;
  }
  return result;
}

export function getRuntimeConfig(): RuntimeConfig {
  const raw = window.__HABITAT_RUNTIME_CONFIG__;

  const basePath = normalizePrefix(raw?.basePath);
  const apiBasePath =
    normalizePrefix(raw?.apiBasePath) || joinPrefixes(basePath, "/api");
  const staticBasePath =
    normalizePrefix(raw?.staticBasePath) || joinPrefixes(basePath, "/_static");

  return {
    basePath,
    apiBasePath: apiBasePath || "/api",
    staticBasePath: staticBasePath || "/_static",
  };
}
