import { TextField as MuiTextField } from "@mui/material";
import { connect, mapProps } from "@formily/react";

// const LabelledTextfield = (props: TextFieldProps) => {
//   const field = useField();
//   const fieldSchema = useFieldSchema();
//   useEffect(() => {
//     console.log("Field:", field);
//     console.log("FieldSchema:", fieldSchema);
//   }, [field, fieldSchema]);
//   return (
//     <>
//       <Stack direction={"row"} alignItems={"center"} spacing={2}>
//         <InputLabel htmlFor={"my-textfield"}>my label</InputLabel>
//         <MuiTextField id="my-textfield" variant="outlined" size={"small"} />
//         <pre>{JSON.stringify(props)}</pre>
//         {/*<pre>{JSON.stringify(Object.keys(field))}</pre>*/}
//         {/*<pre>{JSON.stringify(fieldSchema)}</pre>*/}
//       </Stack>
//     </>
//   );
// };

export const TextField = connect(MuiTextField, mapProps({ title: "label" }));
