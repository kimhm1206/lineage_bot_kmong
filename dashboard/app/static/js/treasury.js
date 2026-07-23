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
    const alliancesData = document.querySelector("#treasury-distribution-alliances-data");
    if (!distributionForm || !usersData || !alliancesData || distributionForm.dataset.treasuryBound) return;
    distributionForm.dataset.treasuryBound = "true";

    const users = JSON.parse(usersData.textContent || "[]");
    const alliances = JSON.parse(alliancesData.textContent || "[]");
    const isAllianceDistribution = distributionForm.dataset.treasuryScope === "1";
    const targets = isAllianceDistribution ? alliances : users;
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
    const allianceSelector = distributionForm.querySelector("[data-treasury-alliance-selector]");

    const formatMoney = (value) => Number(value || 0).toLocaleString("ko-KR");
    const updatePreview = () => {
      const targetCount = Math.max(targets.length - selectedIds.size, 0);
      const requested = Math.max(Number(amountInput?.value || 0), 0);
      const each = targetCount > 0 ? Math.floor(requested / targetCount) : 0;
      const actual = each * targetCount;
      if (recipientCount) {
        recipientCount.textContent = `${targetCount}${isAllianceDistribution ? "개 혈맹" : "명"}`;
      }
      if (perPerson) perPerson.textContent = `${formatMoney(each)} 아데나`;
      if (distributedTotal) distributedTotal.textContent = `${formatMoney(actual)} 아데나`;
      if (remainder) remainder.textContent = `${formatMoney(requested - actual)} 아데나`;
    };

    const renderExcludedTargets = () => {
      excludedInputs.replaceChildren();
      excludedList.replaceChildren();
      const selectedTargets = targets.filter((target) => {
        const targetId = isAllianceDistribution ? target.alliance_id : target.discord_id;
        return selectedIds.has(String(targetId));
      });
      if (!selectedTargets.length) {
        const empty = document.createElement("span");
        empty.textContent = `제외된 ${isAllianceDistribution ? "혈맹" : "혈맹원"}이 없습니다.`;
        excludedList.append(empty);
      } else {
        selectedTargets.forEach((target) => {
          const targetId = isAllianceDistribution ? target.alliance_id : target.discord_id;
          const input = document.createElement("input");
          input.type = "hidden";
          input.name = isAllianceDistribution ? "excluded_alliance_ids" : "excluded_discord_ids";
          input.value = String(targetId);
          excludedInputs.append(input);

          const chip = document.createElement("button");
          chip.type = "button";
          chip.className = "treasury-excluded-chip";
          chip.textContent = `${target.display_name} ×`;
          chip.setAttribute("aria-label", `${target.display_name} 제외 해제`);
          chip.addEventListener("click", () => {
            selectedIds.delete(String(targetId));
            renderExcludedTargets();
          });
          excludedList.append(chip);
        });
      }
      excludedCount.textContent =
        `${selectedTargets.length}${isAllianceDistribution ? "개 혈맹" : "명"} 제외`;
      allianceSelector?.querySelectorAll("[data-alliance-id]").forEach((button) => {
        const isExcluded = selectedIds.has(button.dataset.allianceId);
        button.classList.toggle("is-excluded", isExcluded);
        button.setAttribute("aria-pressed", String(isExcluded));
      });
      updatePreview();
    };

    allianceSelector?.addEventListener("click", (event) => {
      const button = event.target.closest("[data-alliance-id]");
      if (!button) return;
      const allianceId = button.dataset.allianceId;
      if (selectedIds.has(allianceId)) selectedIds.delete(allianceId);
      else selectedIds.add(allianceId);
      renderExcludedTargets();
    });

    openPicker?.addEventListener("click", async () => {
      if (!window.dashboardUserPicker) return;
      const result = await window.dashboardUserPicker.open({
        users,
        multiple: true,
        selectedIds: [...selectedIds],
        title: "분배 제외 혈맹원 선택",
      });
      if (!result) return;
      selectedIds.clear();
      result.forEach((user) => selectedIds.add(String(user.discord_id)));
      renderExcludedTargets();
    });
    amountInput?.addEventListener("input", updatePreview);
    renderExcludedTargets();
  }

  document.addEventListener("dashboard:page-loaded", initializeTreasuryPage);
  initializeTreasuryPage();
})();
