import { z } from "zod";

export default z.object({
  name: z.string(),
  age: z.number().int(),
  email: z.string().email(),
});
