import { useMemo, useState } from "react";
import type { DemoEvent } from "../types";

interface OperationLogProps {
  events: DemoEvent[];
}

export default function OperationLog({ events }: OperationLogProps) {
  const [clearedAt, setClearedAt] = useState(0);
  const visible = useMemo(() => {
    if (clearedAt <= 0) return events;
    return events.slice(clearedAt);
  }, [events, clearedAt]);

  return (
    <section className="panel overflow-hidden">
      <div className="flex items-center justify-between gap-3 border-b border-white/[0.06] px-5 py-4">
        <div>
          <p className="label">Operations</p>
          <h3 className="mt-1 text-lg font-medium text-mist-50">Operation log</h3>
        </div>
        <button
          type="button"
          className="btn-ghost text-xs"
          onClick={() => setClearedAt(events.length)}
        >
          Clear View
        </button>
      </div>

      <div className="max-h-72 overflow-auto px-5 py-4">
        {visible.length === 0 ? (
          <p className="text-sm text-mist-500">No events yet.</p>
        ) : (
          <ul className="space-y-1.5">
            {visible.map((event, index) => (
              <li
                key={`${event.timestamp}-${index}-${event.message}`}
                className="mono flex gap-3 text-[12px] leading-relaxed"
              >
                <span className="shrink-0 text-mist-500">{event.timestamp}</span>
                <span className={`shrink-0 uppercase ${kindColor(event.kind)}`}>
                  {event.kind}
                </span>
                <span className="text-mist-200">{event.message}</span>
              </li>
            ))}
          </ul>
        )}
      </div>
    </section>
  );
}

function kindColor(kind: string): string {
  switch (kind) {
    case "error":
      return "text-failure";
    case "repair":
    case "pull":
    case "push":
      return "text-accent";
    case "node":
      return "text-warning";
    case "lifecycle":
    case "gc":
      return "text-healthy";
    default:
      return "text-mist-400";
  }
}
