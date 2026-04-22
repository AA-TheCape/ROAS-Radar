export type SortDirection = 'asc' | 'desc';

export type SortState<Key extends string> = {
  key: Key;
  direction: SortDirection;
};

export function normalizeQuery(value: string): string {
  return value.trim().toLowerCase();
}

export function matchesQuery(values: Array<string | number | null | undefined>, query: string): boolean {
  const normalizedQuery = normalizeQuery(query);

  if (!normalizedQuery) {
    return true;
  }

  return values.some((value) => String(value ?? '').toLowerCase().includes(normalizedQuery));
}

export function comparePrimitive(
  left: string | number | null | undefined,
  right: string | number | null | undefined
): number {
  if (typeof left === 'number' && typeof right === 'number') {
    return left - right;
  }

  return String(left ?? '').localeCompare(String(right ?? ''), undefined, {
    numeric: true,
    sensitivity: 'base'
  });
}

export function sortRows<T, Key extends string>(
  rows: T[],
  sort: SortState<Key>,
  accessors: Record<Key, (row: T) => string | number | null | undefined>
): T[] {
  const accessor = accessors[sort.key];

  return [...rows].sort((left, right) => {
    const result = comparePrimitive(accessor(left), accessor(right));
    return sort.direction === 'asc' ? result : -result;
  });
}

export function paginateRows<T>(rows: T[], page: number, pageSize: number) {
  const totalPages = Math.max(1, Math.ceil(rows.length / pageSize));
  const currentPage = Math.min(Math.max(page, 1), totalPages);
  const start = (currentPage - 1) * pageSize;

  return {
    currentPage,
    totalPages,
    rows: rows.slice(start, start + pageSize)
  };
}
