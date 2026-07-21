(() => {
  const root = document.documentElement;
  const buttons = document.querySelectorAll("[data-theme-toggle]");
  const labels = document.querySelectorAll("[data-theme-label]");

  function applyTheme(theme) {
    root.dataset.theme = theme;
    localStorage.setItem("dashboard-theme", theme);
    labels.forEach((label) => {
      label.textContent = theme === "dark" ? "Light" : "Dark";
    });
    buttons.forEach((button) => {
      button.setAttribute("aria-pressed", String(theme === "dark"));
    });
  }

  buttons.forEach((button) => {
    button.addEventListener("click", () => {
      applyTheme(root.dataset.theme === "dark" ? "light" : "dark");
    });
  });

  applyTheme(root.dataset.theme === "dark" ? "dark" : "light");
})();
