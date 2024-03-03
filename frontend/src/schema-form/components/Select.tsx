import { isField } from "@formily/core";
import { connect, mapProps } from "@formily/react";
import { Autocomplete as MuiAutocomplete, Box, TextField } from "@mui/material";

export const Select = connect(
  MuiAutocomplete,
  mapProps({ dataSource: "options" }, (props, field) => {
    return {
      ...props,
      onChange: (_event, value) => {
        if (isField(field)) {
          field.setValue(value);
        }
      },
      renderInput: (params) =>
        isField(field) ? (
          <TextField {...params} label={field.title as string} />
        ) : (
          <Box>{field.title}</Box>
        ),
    };
  })
);
