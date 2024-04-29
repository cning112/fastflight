import { z } from "zod";

export default z.object({
  strategy: z.object({ id: z.string(), namespace: z.string() }),
  alloc: z.number(),
});
