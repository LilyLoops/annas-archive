import * as DarkReader from 'darkreader';


const darkReaderOptions = {
  brightness: 100,
  contrast: 90,
  sepia: 0,
  grayscale: 0,
  useFont: false,
  textStroke: 0,
  engine: "dynamicTheme",
  darkSchemeBackgroundColor: "#181a1b",
  darkSchemeTextColor: "#e8e6e3",
  scrollbarColor: "auto",
  selectionColor: "auto",
  styleSystemControls: false,
  darkColorScheme: "Default",
  immediateModify: false,
};

const updateThemeButtons = () => {
  const lightButton = document.getElementById("light-button");
  if(!lightButton) {
    setTimeout(()=>updateThemeButtons(),500);
    return;
  }
  const darkButton = document.getElementById("dark-button");

  if (DarkReader.isEnabled()) {
    lightButton.classList.remove("hidden");
    darkButton.classList.add("hidden");
  } else {
    lightButton.classList.add("hidden");
    darkButton.classList.remove("hidden");
  }
};

window.handleThemeSwitch = (theme) => {
  switch (theme) {
    case "light":
      localStorage.setItem("theme", "light");
      DarkReader.disable();
      break;
    case "dark":
      localStorage.setItem("theme", "dark");
      DarkReader.enable(darkReaderOptions);
      break;
    default:
      DarkReader.auto(darkReaderOptions);
      localStorage.setItem(
        "theme",
        DarkReader.isEnabled() ? "dark" : "light"
      );
      break;
  }
  updateThemeButtons();
};
const theme = localStorage.getItem("theme");
handleThemeSwitch(theme);
