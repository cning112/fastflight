import type { Meta, StoryObj } from "@storybook/react";
import { BespokeForm } from "./BespokeForm.tsx";

const meta: Meta<typeof BespokeForm> = {
  component: BespokeForm,
  parameters: {
    layout: "center",
  },
  decorators: [
    (Story) => (
      <div style={{ width: "40%" }}>
        <Story />
      </div>
    ),
  ],
};

export default meta;
type Story = StoryObj<typeof BespokeForm>;

export const Simple: Story = {
  args: {},
};
