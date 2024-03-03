import {
  Button,
  ButtonGroup,
  Divider,
  FormControl,
  IconButton,
  InputAdornment,
  InputBase,
  Paper,
  Stack,
  TextField,
} from "@mui/material";
import MenuIcon from "@mui/icons-material/Menu";
import SearchIcon from "@mui/icons-material/Search";
import DirectionsIcon from "@mui/icons-material/Directions";
import { useState } from "react";

const StartTypeButtonGroup = () => {
  const [type, setType] = useState("date");

  return (
    <ButtonGroup
      variant="text"
      aria-label="Start type"
      disableElevation={true}
      size={"small"}
    >
      <Button
        onClick={() => setType("date")}
        variant={type === "date" ? "contained" : "outlined"}
      >
        Date
      </Button>
      <Button
        onClick={() => setType("tenor")}
        variant={type === "tenor" ? "contained" : "outlined"}
      >
        Tenor
      </Button>
      <Button
        onClick={() => setType("imm")}
        variant={type === "imm" ? "contained" : "outlined"}
      >
        IMM
      </Button>
    </ButtonGroup>
  );
};

const CustomizedInputBase = () => {
  return (
    <Paper
      component="form"
      sx={{ p: "2px 4px", display: "flex", alignItems: "stretch", width: 600 }}
    >
      <IconButton sx={{ p: "10px" }} aria-label="menu">
        <MenuIcon />
      </IconButton>

      <InputBase
        sx={{ ml: 1, flex: 1 }}
        placeholder="Search Google Maps"
        inputProps={{ "aria-label": "search google maps" }}
      />

      <StartTypeButtonGroup />

      <Divider sx={{ height: 28, m: 0.5 }} orientation="vertical" />
      <Button variant={"contained"}>Date</Button>

      <Divider sx={{ height: 28, m: 0.5 }} orientation="vertical" />
      <IconButton type="button" sx={{ p: "10px" }} aria-label="search">
        <SearchIcon />
      </IconButton>
      <Divider sx={{ height: 28, m: 0.5 }} orientation="vertical" />
      <IconButton color="primary" sx={{ p: "10px" }} aria-label="directions">
        <DirectionsIcon />
      </IconButton>
    </Paper>
  );
};

export const BespokeForm = () => {
  const [flag, setFlag] = useState(true);

  return (
    <form>
      <Stack spacing={2}>
        <CustomizedInputBase />
        <FormControl>
          <TextField
            label={"start"}
            size={"small"}
            helperText={"date, tenor or IMM"}
            InputProps={{
              endAdornment: (
                <>
                  <InputAdornment position="end">
                    <StartTypeButtonGroup />
                  </InputAdornment>
                </>
              ),
            }}
          ></TextField>
        </FormControl>

        <FormControl size={"small"}>
          <TextField label={"end"} size={"small"}></TextField>
        </FormControl>
      </Stack>
    </form>
  );
};
