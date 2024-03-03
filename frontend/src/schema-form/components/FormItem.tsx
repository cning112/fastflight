import { connect, mapProps } from "@formily/react";
import { Box } from "@mui/material";

export const FormItem = connect(
  Box,
  mapProps((props) => {
    return { ...props, sx: { ...props.sx, mb: 3 } };
  })
);
