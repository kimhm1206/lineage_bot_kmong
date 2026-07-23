(() => {
  function initializeTreasuryPage() {
    const form = document.querySelector("[data-treasury-entry-form]");
    if (form && !form.dataset.treasuryBound) {
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

    const distributionForm = document.querySelector("[data-treasury-distribution-form]");
    const usersData = document.querySelector("#treasury-distribution-users-data");
    if (!distributionForm || !usersData || distributionForm.dataset.treasuryBound) return;
    distributionForm.dataset.treasuryBound = "true";

    const users = JSON.parse(usersData.textContent || "[]");
    const selectedIds = new Set();
    const excludedInputs = distributionForm.querySelector("[data-treasury-excluded-inputs]");
    const excludedList = distributionForm.querySelector("[data-treasury-excluded-list]");
    const excludedCount = distributionForm.querySelector("[data-treasury-excluded-count]");
    const recipientCount = distributionForm.querySelector("[data-treasury-recipient-count]");
    const amountInput = distributionForm.querySelector("[data-treasury-distribution-amount]");
    const perPerson = distributionForm.querySelector("[data-treasury-per-person]");
    const distributedTotal = distributionForm.querySelector("[data-treasury-distributed-total]");
    const remainder = distributionForm.querySelector("[data-treasury-remainder]");
    const openPicker = distributionForm.querySelector("[data-treasury-excluded-open]");

    const formatMoney = (value) => Number(value || 0).toLocaleString("ko-KR");
    const updatePreview = () => {
      const targetCount = Math.max(users.length - selectedIds.size, 0);
      const requested = Math.max(Number(amountInput?.value || 0), 0);
      const each = targetCount > 0 ? Math.floor(requested / targetCount) : 0;
      const actual = each * targetCount;
      if (recipientCount) recipientCount.textContent = `${targetCount}명`;
      if (perPerson) perPerson.textContent = `${formatMoney(each)} 아데나`;
      if (distributedTotal) distributedTotal.textContent = `${formatMoney(actual)} 아데나`;
      if (remainder) remainder.textContent = `${formatMoney(requested - actual)} 아데나`;
    };

    const renderExcludedUsers = () => {
      excludedInputs.replaceChildren();
      excludedList.replaceChildren();
      const selectedUsers = users.filter((user) => selectedIds.has(String(user.discord_id)));
      if (!selectedUsers.length) {
        const empty = document.createElement("span");
        empty.textContent = "제외된 유저가 없습니다.";
        excludedList.append(empty);
      } else {
        selectedUsers.forEach((user) => {
          const input = document.createElement("input");
          input.type = "hidden";
          input.name = "excluded_discord_ids";
          input.value = String(user.discord_id);
          excludedInputs.append(input);

          const chip = document.createElement("button");
          chip.type = "button";
          chip.className = "treasury-excluded-chip";
          chip.textContent = `${user.display_name} ×`;
          chip.setAttribute("aria-label", `${user.display_name} 제외 해제`);
          chip.addEventListener("click", () => {
            selectedIds.delete(String(user.discord_id));
            renderExcludedUsers();
          });
          excludedList.append(chip);
        });
      }
      excludedCount.textContent = `${selectedUsers.length}명 제외`;
      updatePreview();
    };

    openPicker?.addEventListener("click", async () => {
      if (!window.dashboardUserPicker) return;
      const result = await window.dashboardUserPicker.open({
        users,
        multiple: true,
        selectedIds: [...selectedIds],
        title: "분배 제외자 선택",
      });
      if (!result) return;
      selectedIds.clear();
      result.forEach((user) => selectedIds.add(String(user.discord_id)));
      renderExcludedUsers();
    });
    amountInput?.addEventListener("input", updatePreview);
    renderExcludedUsers();
  }

  document.addEventListener("dashboard:page-loaded", initializeTreasuryPage);
  initializeTreasuryPage();
})();
