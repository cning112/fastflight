import { z } from "zod";

export default z.object({ id: z.string(), namespace: z.string() });
