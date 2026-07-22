(() => {
  const form = document.querySelector("[data-manager-form]");
  if (!form) return;

  const scope = form.querySelector("[data-scope-select]");
  const allianceField = form.querySelector("[data-alliance-field]");
  const allianceSelect = allianceField?.querySelector("select");

  function syncScope() {
    const needsAlliance = scope?.value === "2";
    allianceField?.classList.toggle("is-hidden", !needsAlliance);
    if (allianceSelect) allianceSelect.required = needsAlliance;
  }

  scope?.addEventListener("change", syncScope);
  syncScope();
})();
