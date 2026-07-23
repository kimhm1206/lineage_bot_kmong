(() => {
  function initializeSettingsPage() {
    const usersData = document.querySelector("#page-users-data");
    const forms = [...document.querySelectorAll("[data-manager-form]")].filter(
      (form) => !form.dataset.settingsBound,
    );
    if (!usersData || !forms.length) return;

    const users = JSON.parse(usersData.textContent || "[]");
    const assignmentsData = document.querySelector("#manager-assignments-data");
    const assignments = assignmentsData ? JSON.parse(assignmentsData.textContent || "{}") : {};

    function excludedIdsFor(form) {
      if (form.dataset.pickerScope === "alliance") return assignments.alliance || [];
      if (form.dataset.pickerScope === "clan") {
        const allianceId = form.querySelector('input[name="alliance_id"]:checked')?.value;
        return assignments.clans?.[allianceId] || [];
      }
      try {
        return JSON.parse(form.dataset.excludedIds || "[]");
      } catch {
        return [];
      }
    }

    function setupUserField(form) {
      const field = form.querySelector("[data-user-picker-field]");
      const openButton = field?.querySelector("[data-user-picker-open]");
      const hiddenInput = field?.querySelector("[data-user-id]");
      const name = field?.querySelector("[data-user-picker-name]");
      const meta = field?.querySelector("[data-user-picker-meta]");
      const error = field?.querySelector("[data-picker-error]");
      if (!field || !openButton || !hiddenInput || !name || !meta || !error) return;
      form.dataset.settingsBound = "true";

      function clearSelection() {
        hiddenInput.value = "";
        name.textContent = "유저 선택";
        meta.textContent = "검색해서 선택";
        openButton.classList.remove("has-selection");
      }

      openButton.addEventListener("click", async () => {
        if (!window.dashboardUserPicker) return;
        const result = await window.dashboardUserPicker.open({
          users,
          multiple: false,
          title: form.dataset.pickerScope === "static" ? "혈맹 경리 선택" : "운영 담당자 선택",
          selectedIds: hiddenInput.value ? [hiddenInput.value] : [],
          excludedIds: excludedIdsFor(form),
        });
        if (!result?.length) return;
        const selected = result[0];
        hiddenInput.value = String(selected.discord_id);
        name.textContent = selected.display_name;
        meta.textContent = selected.username || String(selected.discord_id);
        openButton.classList.add("has-selection");
        error.classList.add("is-hidden");
      });

      form.querySelectorAll('input[name="alliance_id"]').forEach((radio) => {
        radio.addEventListener("change", clearSelection);
      });
      form.addEventListener("submit", (event) => {
        if (hiddenInput.value) return;
        event.preventDefault();
        error.classList.remove("is-hidden");
        openButton.focus();
      });
    }

    forms.forEach(setupUserField);
  }

  document.addEventListener("dashboard:page-loaded", initializeSettingsPage);
  initializeSettingsPage();
})();
