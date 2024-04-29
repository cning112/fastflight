import { z } from "zod";

export default z.object({
  allocations: z.array(
    z.object({
      strategy: z.object({ id: z.string(), namespace: z.string() }),
      alloc: z.number(),
    }),
  ),
  start: z.string().datetime(),
  end: z.string().datetime(),
});
