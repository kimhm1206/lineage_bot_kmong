(() => {
  function initializeTreasuryPage() {
    const form = document.querySelector("[data-treasury-entry-form]");
    if (!form || form.dataset.treasuryBound) return;
    form.dataset.treasuryBound = "true";

    const directionInputs = [...form.querySelectorAll('input[name="direction"]')];
    const categoryFields = [...form.querySelectorAll("[data-category-field]")];
    const submitButton = form.querySelector("[data-treasury-submit]");

    const syncDirection = () => {
      const direction = directionInputs.find((input) => input.checked)?.value || "1";
      categoryFields.forEach((field) => {
        const active = field.dataset.categoryField === direction;
        field.classList.toggle("is-hidden", !active);
        const select = field.querySelector("select");
        if (select) select.disabled = !active;
      });
      if (submitButton) submitButton.textContent = direction === "1" ? "입금 기록" : "출금 기록";
      form.dataset.direction = direction;
    };

    directionInputs.forEach((input) => input.addEventListener("change", syncDirection));
    syncDirection();
  }

  document.addEventListener("dashboard:page-loaded", initializeTreasuryPage);
  initializeTreasuryPage();
})();
