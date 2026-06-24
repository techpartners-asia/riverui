const FALLBACK_PAYLOAD = '{\n  "approved": true\n}';

// defaultPayloadFromCel derives a sensible starter payload from a signal
// term's CEL expression so the emitted signal satisfies common conditions out
// of the box (e.g. `payload.ok` -> {"ok": true}, `payload.score >= 90` ->
// {"score": 90}, `payload.action == "go"` -> {"action": "go"}). Falls back to
// {"approved": true} when no `payload.<field>` reference is found.
export const defaultPayloadFromCel = (cel?: string): string => {
  if (!cel) return FALLBACK_PAYLOAD;
  const obj: Record<string, unknown> = {};
  const re =
    /payload\.([A-Za-z_]\w*)\s*(==|!=|>=|<=|>|<)?\s*("(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'|true|false|-?\d+(?:\.\d+)?)?/g;
  let match: null | RegExpExecArray;
  while ((match = re.exec(cel)) !== null) {
    const field = match[1];
    const op = match[2];
    const raw = match[3];
    if (field in obj) continue;
    let value: unknown = true;
    if (raw === "true") value = true;
    else if (raw === "false") value = op === "!=";
    else if (raw !== undefined && /^-?\d/.test(raw)) value = Number(raw);
    else if (raw !== undefined) {
      const inner = raw.slice(1, -1);
      value = op === "!=" && inner === "" ? "value" : inner;
    }
    obj[field] = value;
  }
  if (Object.keys(obj).length === 0) return FALLBACK_PAYLOAD;
  return JSON.stringify(obj, null, 2);
};
