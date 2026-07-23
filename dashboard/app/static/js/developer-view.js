(() => {
  const modal = document.getElementById("developer-view-modal");
  const openButton = document.querySelector("[data-developer-view-open]");
  if (!modal || !openButton) return;

  const loading = modal.querySelector("[data-developer-view-loading]");
  const content = modal.querySelector("[data-developer-view-content]");
  const roleGrid = modal.querySelector("[data-developer-view-roles]");
  const allianceStep = modal.querySelector("[data-developer-view-alliance-step]");
  const allianceGrid = modal.querySelector("[data-developer-view-alliances]");
  const empty = modal.querySelector("[data-developer-view-empty]");
  const errorBox = modal.querySelector("[data-developer-view-error]");
  const applyButton = modal.querySelector("[data-developer-view-apply]");
  const closeButtons = modal.querySelectorAll("[data-developer-view-close]");
  const roleDescriptions = {
    developer: "모든 서버와 개발자 도구",
    owner: "서버 전체 운영과 모든 혈맹",
    alliance_manager: "연합 운영과 선택 혈맹 조회",
    clan_manager: "선택 혈맹 운영과 설정",
    clan_accountant: "선택 혈맹 정산과 가계부",
    user: "개인 조회와 선택 혈맹 정보",
  };
  let selectedMode = "";
  let selectedAllianceId = null;
  let modes = [];
  let alliances = [];

  function isClanMode(mode) {
    return modes.find((item) => item.value === mode)?.requires_alliance === true;
  }

  function setError(message = "") {
    errorBox.textContent = message;
    errorBox.hidden = !message;
  }

  function updateApplyState() {
    const needsAlliance = isClanMode(selectedMode);
    applyButton.disabled = !selectedMode || (needsAlliance && !selectedAllianceId);
  }

  function renderRoles() {
    roleGrid.replaceChildren();
    modes.forEach((mode) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "developer-view-role";
      button.classList.toggle("is-selected", mode.value === selectedMode);
      button.dataset.mode = mode.value;

      const label = document.createElement("strong");
      label.textContent = mode.label;
      const description = document.createElement("small");
      description.textContent = roleDescriptions[mode.value] || "";
      const marker = document.createElement("span");
      marker.className = "developer-view-selection-marker";
      marker.textContent = mode.value === selectedMode ? "선택됨" : "";
      button.append(label, description, marker);
      button.addEventListener("click", () => {
        selectedMode = mode.value;
        if (!isClanMode(selectedMode)) selectedAllianceId = null;
        renderRoles();
        renderAlliances();
        updateApplyState();
      });
      roleGrid.append(button);
    });
  }

  function renderAlliances() {
    const visible = isClanMode(selectedMode);
    allianceStep.hidden = !visible;
    allianceGrid.replaceChildren();
    if (!visible) return;

    empty.hidden = alliances.length > 0;
    alliances.forEach((alliance) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "developer-view-alliance";
      button.classList.toggle(
        "is-selected",
        Number(alliance.alliance_id) === Number(selectedAllianceId),
      );
      const label = document.createElement("strong");
      label.textContent = alliance.alliance_name;
      const marker = document.createElement("span");
      marker.textContent = Number(alliance.alliance_id) === Number(selectedAllianceId)
        ? "선택됨"
        : "선택";
      button.append(label, marker);
      button.addEventListener("click", () => {
        selectedAllianceId = Number(alliance.alliance_id);
        renderAlliances();
        updateApplyState();
      });
      allianceGrid.append(button);
    });
  }

  async function loadOptions() {
    loading.hidden = false;
    content.hidden = true;
    applyButton.disabled = true;
    setError();
    try {
      const response = await fetch("/auth/developer-view/options", {
        headers: { Accept: "application/json" },
      });
      const payload = await response.json();
      if (!response.ok || !payload.ok) {
        throw new Error(payload.detail || payload.message || "권한 정보를 불러오지 못했습니다.");
      }
      modes = Array.isArray(payload.modes) ? payload.modes : [];
      alliances = Array.isArray(payload.alliances) ? payload.alliances : [];
      selectedMode = payload.active_mode || "developer";
      selectedAllianceId = payload.active_alliance_id
        ? Number(payload.active_alliance_id)
        : null;
      renderRoles();
      renderAlliances();
      updateApplyState();
      content.hidden = false;
    } catch (error) {
      content.hidden = false;
      setError(error.message || "권한 정보를 불러오지 못했습니다.");
    } finally {
      loading.hidden = true;
    }
  }

  function openModal() {
    modal.classList.add("is-open");
    modal.setAttribute("aria-hidden", "false");
    document.body.classList.add("modal-open");
    loadOptions();
  }

  function closeModal() {
    modal.classList.remove("is-open");
    modal.setAttribute("aria-hidden", "true");
    document.body.classList.remove("modal-open");
    openButton.focus();
  }

  async function applyView() {
    if (applyButton.disabled) return;
    applyButton.disabled = true;
    applyButton.textContent = "변경 중";
    setError();
    try {
      const response = await fetch("/auth/developer-view", {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          mode: selectedMode,
          alliance_id: isClanMode(selectedMode) ? selectedAllianceId : null,
        }),
      });
      const payload = await response.json();
      if (!response.ok || !payload.ok) {
        throw new Error(payload.detail || payload.message || "권한을 변경하지 못했습니다.");
      }
      window.location.assign("/");
    } catch (error) {
      setError(error.message || "권한을 변경하지 못했습니다.");
      applyButton.disabled = false;
      applyButton.textContent = "이 권한으로 보기";
    }
  }

  openButton.addEventListener("click", openModal);
  closeButtons.forEach((button) => button.addEventListener("click", closeModal));
  applyButton.addEventListener("click", applyView);
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && modal.classList.contains("is-open")) closeModal();
  });
})();
