import type { Meta, StoryObj } from "@storybook/react";

import { SchemaForm } from "./SchemaForm.tsx";

const meta: Meta<typeof SchemaForm> = {
  component: SchemaForm,
};

export default meta;
type Story = StoryObj<typeof SchemaForm>;

export const Simple: Story = {
  args: {
    schema: {
      type: "object",
      properties: {
        username: {
          type: "string",
          title: "Username",
          required: true,
          "x-decorator": "FormItem",
          "x-component": "TextField",
        },
        gender: {
          type: "string",
          title: "Gender",
          required: true,
          "x-decorator": "FormItem",
          "x-component": "Select",
          enum: [
            {
              label: "male",
              value: 1,
            },
            {
              label: "female",
              value: 2,
            },
            {
              label: "third gender",
              value: 3,
            },
          ],
        },
      },
    },
    initialValues: { username: "admin", gender: "female" },
  },
};

export const Secondary: Story = {
  args: {
    ...Simple.args,
  },
};
