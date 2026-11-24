import process from "node:process";

export type TuiTone = "default" | "brand" | "success" | "warn" | "error" | "info" | "muted";

// eslint-disable-next-line no-control-regex
const ANSI_REGEX = /\x1b\[[0-9;]*m/g;

export function supportsColor(stream: NodeJS.WriteStream = process.stdout): boolean {
  if (process.env.NO_COLOR !== undefined && process.env.NO_COLOR !== "") {
    return false;
  }
  if (process.env.FORCE_COLOR !== undefined && process.env.FORCE_COLOR !== "0") {
    return true;
  }
  return Boolean(stream?.isTTY);
}

export function stripAnsi(text: string): string {
  return text.replace(ANSI_REGEX, "");
}

export function visibleLength(text: string): number {
  return stripAnsi(text).length;
}

export function padVisibleEnd(text: string, width: number): string {
  const len = visibleLength(text);
  return len >= width ? text : text + " ".repeat(width - len);
}

export function padVisibleStart(text: string, width: number): string {
  const len = visibleLength(text);
  return len >= width ? text : " ".repeat(width - len) + text;
}

export function truncateVisible(text: string, maxWidth: number): string {
  if (maxWidth <= 0) return "";
  if (visibleLength(text) <= maxWidth) return text;
  const plain = stripAnsi(text);
  if (maxWidth <= 1) return plain.slice(0, maxWidth);
  return `${plain.slice(0, maxWidth - 1)}…`;
}

export function getTerminalWidth(maxWidth = 104, minWidth = 60): number {
  const cols = process.stdout?.columns;
  if (typeof cols === "number" && cols > 0) {
    return Math.max(minWidth, Math.min(cols - 2, maxWidth));
  }
  return Math.min(92, maxWidth);
}

function wrapAnsi(code: string, text: string, colorEnabled = supportsColor()): string {
  if (!colorEnabled) return text;
  return `\x1b[${code}m${text}\x1b[0m`;
}

export const style = {
  bold: (text: string, c?: boolean) => wrapAnsi("1", text, c),
  dim: (text: string, c?: boolean) => wrapAnsi("2", text, c),
  green: (text: string, c?: boolean) => wrapAnsi("32", text, c),
  brightGreen: (text: string, c?: boolean) => wrapAnsi("92", text, c),
  yellow: (text: string, c?: boolean) => wrapAnsi("33", text, c),
  red: (text: string, c?: boolean) => wrapAnsi("31", text, c),
  cyan: (text: string, c?: boolean) => wrapAnsi("36", text, c),
  blue: (text: string, c?: boolean) => wrapAnsi("34", text, c),
  magenta: (text: string, c?: boolean) => wrapAnsi("35", text, c),
  gray: (text: string, c?: boolean) => wrapAnsi("90", text, c),
  white: (text: string, c?: boolean) => wrapAnsi("97", text, c),
};

export function toneColor(tone: TuiTone, text: string, colorEnabled = supportsColor()): string {
  switch (tone) {
    case "brand":
      return wrapAnsi("1;92", text, colorEnabled);
    case "success":
      return wrapAnsi("1;32", text, colorEnabled);
    case "warn":
      return wrapAnsi("1;33", text, colorEnabled);
    case "error":
      return wrapAnsi("1;31", text, colorEnabled);
    case "info":
      return wrapAnsi("1;36", text, colorEnabled);
    case "muted":
      return wrapAnsi("2", text, colorEnabled);
    default:
      return text;
  }
}

export function badge(label: string, tone: TuiTone, colorEnabled = supportsColor()): string {
  const icons: Record<TuiTone, string> = {
    brand: "◆",
    success: "✓",
    warn: "⚠",
    error: "✗",
    info: "●",
    muted: "○",
    default: "•",
  };
  const text = `${icons[tone]} ${label}`;
  return toneColor(tone, text, colorEnabled);
}

export function renderBanner(subtitle?: string, colorEnabled = supportsColor()): string {
  const title = `${toneColor("brand", "◆ MongoVault", colorEnabled)} ${style.dim("v1.0.0", colorEnabled)}`;
  const sub = subtitle
    ? `  ${style.dim("·", colorEnabled)}  ${style.cyan(subtitle, colorEnabled)}`
    : "";
  return `${title}${sub}`;
}

export interface KeyValueRow {
  label: string;
  value: string;
  tone?: TuiTone;
}

export function renderKeyValue(
  rows: KeyValueRow[],
  options: { indent?: number; color?: boolean } = {},
): string[] {
  const c = options.color ?? supportsColor();
  const indent = " ".repeat(options.indent ?? 0);
  const labelWidth = rows.reduce((max, r) => Math.max(max, visibleLength(r.label)), 0);

  return rows.map((row) => {
    const paddedLabel = padVisibleEnd(row.label, labelWidth);
    const styledLabel = style.dim(`${paddedLabel}:`, c);
    const styledVal = row.tone ? toneColor(row.tone, row.value, c) : row.value;
    return `${indent}${styledLabel}  ${styledVal}`;
  });
}

export interface PanelOptions {
  title: string;
  tone?: TuiTone;
  width?: number;
  color?: boolean;
  subtitle?: string;
}

export function renderPanel(lines: string[], options: PanelOptions): string {
  const c = options.color ?? supportsColor();
  const tone = options.tone ?? "default";
  const contentMax = lines.reduce((m, line) => Math.max(m, visibleLength(line)), 0);
  const titlePlain = options.subtitle ? `${options.title} · ${options.subtitle}` : options.title;
  const minInner = Math.max(contentMax, visibleLength(titlePlain) + 4, 44);
  const maxAllowed = (options.width ?? getTerminalWidth()) - 4;
  const innerWidth = Math.max(44, Math.min(Math.max(minInner, 56), Math.max(maxAllowed, minInner)));

  const borderFn = (s: string) => {
    if (tone === "error") return style.red(s, c);
    if (tone === "warn") return style.yellow(s, c);
    if (tone === "success" || tone === "brand") return style.green(s, c);
    if (tone === "info") return style.cyan(s, c);
    return style.gray(s, c);
  };

  const titleFormatted =
    tone === "default" ? style.bold(options.title, c) : toneColor(tone, options.title, c);
  const subFormatted = options.subtitle ? ` ${style.dim(`· ${options.subtitle}`, c)}` : "";
  const headerContent = ` ${titleFormatted}${subFormatted} `;
  const headerFill = Math.max(1, innerWidth + 2 - 1 - visibleLength(headerContent));

  const top = `${borderFn("╭─")}${headerContent}${borderFn(`${"─".repeat(headerFill)}╮`)}`;
  const bottom = borderFn(`╰${"─".repeat(innerWidth + 2)}╯`);

  const body = lines.map((line) => {
    const padded = padVisibleEnd(line, innerWidth);
    return `${borderFn("│")} ${padded} ${borderFn("│")}`;
  });

  return [top, ...body, bottom].join("\n");
}

export interface TableColumn {
  header: string;
  align?: "left" | "right";
  maxWidth?: number;
}

export function renderTable(
  columns: TableColumn[],
  rows: string[][],
  options: { color?: boolean; indent?: number } = {},
): string {
  const c = options.color ?? supportsColor();
  const indent = " ".repeat(options.indent ?? 0);

  const widths = columns.map((col, idx) => {
    let w = visibleLength(col.header);
    for (const row of rows) {
      w = Math.max(w, visibleLength(row[idx] ?? ""));
    }
    if (col.maxWidth && w > col.maxWidth) {
      w = col.maxWidth;
    }
    return w;
  });

  const formatRow = (cells: string[], isHeader = false): string => {
    const formatted = columns.map((col, idx) => {
      const width = widths[idx]!;
      const raw = cells[idx] ?? "";
      const clipped = visibleLength(raw) > width ? truncateVisible(raw, width) : raw;
      const aligned =
        col.align === "right" ? padVisibleStart(clipped, width) : padVisibleEnd(clipped, width);
      return isHeader ? style.bold(style.cyan(aligned, c), c) : aligned;
    });
    return `${indent}${formatted.join("  ")}`;
  };

  const sepWidth = widths.reduce((sum, w) => sum + w, 0) + Math.max(0, columns.length - 1) * 2;
  const separator = `${indent}${style.gray("─".repeat(sepWidth), c)}`;

  const out = [
    formatRow(
      columns.map((col) => col.header),
      true,
    ),
    separator,
  ];
  for (const row of rows) {
    out.push(formatRow(row, false));
  }
  return out.join("\n");
}

export function renderBar(ratio: number, width = 16, colorEnabled = supportsColor()): string {
  const clamped = Math.max(0, Math.min(1, Number.isFinite(ratio) ? ratio : 0));
  const filled = Math.round(clamped * width);
  const empty = Math.max(0, width - filled);
  return `${style.green("█".repeat(filled), colorEnabled)}${style.gray("░".repeat(empty), colorEnabled)}`;
}

export function renderErrorBox(
  title: string,
  error: unknown,
  options: { showStack?: boolean; color?: boolean } = {},
): string {
  const c = options.color ?? supportsColor();
  const err = error instanceof Error ? error : new Error(String(error));
  const messageLines = err.message.split("\n").map((line) => line.trimEnd());
  const lines = [`${badge(title, "error", c)}`, ...messageLines.map((l) => `  ${l}`)];
  if (options.showStack && err.stack) {
    const stackLines = err.stack.split("\n").slice(1, 6);
    if (stackLines.length > 0) {
      lines.push("");
      for (const s of stackLines) {
        lines.push(style.dim(s, c));
      }
    }
  }
  return renderPanel(lines, { title: "Error", tone: "error", color: c });
}

const SPINNER_FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"] as const;

export interface Spinner {
  update(message: string): void;
  succeed(message?: string): void;
  fail(message?: string): void;
  stop(): void;
}

export function createSpinner(
  initialMessage: string,
  options: { enabled?: boolean; stream?: NodeJS.WriteStream } = {},
): Spinner {
  const stream = options.stream ?? process.stderr;
  const enabled = options.enabled ?? Boolean(stream?.isTTY && supportsColor(stream));
  const startedAt = Date.now();
  let message = initialMessage;
  let frameIndex = 0;
  let timer: NodeJS.Timeout | null = null;
  let stopped = false;

  const formatElapsed = () => `${((Date.now() - startedAt) / 1000).toFixed(1)}s`;

  const renderFrame = () => {
    if (!enabled || stopped) return;
    const frame = style.cyan(SPINNER_FRAMES[frameIndex % SPINNER_FRAMES.length]!, true);
    frameIndex++;
    stream.write(`\r\x1b[2K${frame} ${message} ${style.dim(`(${formatElapsed()})`, true)}`);
  };

  if (enabled) {
    renderFrame();
    timer = setInterval(renderFrame, 80);
    timer.unref();
  }

  const clearLine = () => {
    if (timer) {
      clearInterval(timer);
      timer = null;
    }
    if (enabled && !stopped) {
      stream.write("\r\x1b[2K");
    }
    stopped = true;
  };

  return {
    update(nextMessage: string) {
      message = nextMessage;
      renderFrame();
    },
    succeed(finalMessage?: string) {
      const wasEnabled = enabled && !stopped;
      clearLine();
      if (wasEnabled) {
        stream.write(
          `${badge(finalMessage ?? message, "success", true)} ${style.dim(`(${formatElapsed()})`, true)}\n`,
        );
      }
    },
    fail(finalMessage?: string) {
      const wasEnabled = enabled && !stopped;
      clearLine();
      if (wasEnabled) {
        stream.write(
          `${badge(finalMessage ?? message, "error", true)} ${style.dim(`(${formatElapsed()})`, true)}\n`,
        );
      }
    },
    stop() {
      clearLine();
    },
  };
}
