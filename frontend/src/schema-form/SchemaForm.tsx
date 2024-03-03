import { createForm } from "@formily/core";
import { createSchemaField, FormConsumer, FormProvider } from "@formily/react";
import { Typography } from "@mui/material";
import { useMemo } from "react";
import { FormItem, Select, Switch, TextField } from "./components";

const SchemaField = createSchemaField({
  components: {
    Select,
    Switch,
    TextField,
    FormItem,
  },
});

export const SchemaForm = (props: {
  schema: Record<string, unknown>;
  initialValues?: Record<string, unknown>;
}) => {
  const form = useMemo(
    () =>
      createForm({ validateFirst: true, initialValues: props.initialValues }),
    []
  );

  return (
    <FormProvider form={form}>
      <>
        <Typography variant={"h5"}>Schema</Typography>{" "}
        <pre>{JSON.stringify(props.schema)}</pre>
        {props.initialValues && (
          <>
            <Typography variant={"h5"}>InitValues</Typography>
            <pre>{JSON.stringify(props.initialValues)}</pre>
          </>
        )}
        <Typography variant={"h5"}>Form Data</Typography>
        <FormConsumer>
          {(f) => <pre>{JSON.stringify(f.values)}</pre>}
        </FormConsumer>
        <SchemaField schema={props.schema} />
      </>
    </FormProvider>
  );
};
