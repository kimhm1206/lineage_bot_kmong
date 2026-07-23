(() => {
  const sidebar = document.querySelector("[data-sidebar]");
  if (!sidebar) return;

  const groupTriggers = [...sidebar.querySelectorAll("[data-nav-group-trigger]")];
  const groupPanels = [...sidebar.querySelectorAll("[data-nav-group-panel]")];
  const mobileToggle = sidebar.querySelector("[data-nav-toggle]");
  const collapseButton = sidebar.querySelector("[data-sidebar-collapse]");
  const mobileQuery = window.matchMedia("(max-width: 980px)");
  const collapsedKey = "lineage-dashboard:sidebar-collapsed";
  const activeGroup = sidebar.dataset.activeGroup || groupTriggers[0]?.dataset.navGroupTrigger || "";
  let selectedGroup = activeGroup;

  function readCollapsedPreference() {
    try {
      const saved = localStorage.getItem(collapsedKey);
      return saved === null ? true : saved === "true";
    } catch {
      return true;
    }
  }

  function saveCollapsedPreference(isCollapsed) {
    try {
      localStorage.setItem(collapsedKey, String(isCollapsed));
    } catch {
      // Navigation remains usable when browser storage is unavailable.
    }
  }

  function selectGroup(groupId) {
    if (!groupId) return;
    selectedGroup = groupId;
    groupTriggers.forEach((trigger) => {
      const isSelected = trigger.dataset.navGroupTrigger === groupId;
      trigger.classList.toggle("is-active", isSelected);
    });
    groupPanels.forEach((panel) => {
      const isSelected = panel.dataset.navGroupPanel === groupId;
      panel.hidden = !isSelected;
      panel.classList.toggle("is-current", isSelected);
    });
    updateExpandedState();
  }

  function updateExpandedState() {
    const panelIsVisible = mobileQuery.matches
      ? sidebar.classList.contains("is-open")
      : !sidebar.classList.contains("is-collapsed");
    groupTriggers.forEach((trigger) => {
      const isSelected = trigger.dataset.navGroupTrigger === selectedGroup;
      trigger.setAttribute("aria-expanded", String(isSelected && panelIsVisible));
    });
  }

  function setDesktopCollapsed(isCollapsed, { persist = true } = {}) {
    if (mobileQuery.matches) return;
    sidebar.classList.toggle("is-collapsed", isCollapsed);
    updateExpandedState();
    if (persist) saveCollapsedPreference(isCollapsed);
  }

  function setMobileOpen(isOpen) {
    sidebar.classList.toggle("is-open", isOpen);
    updateExpandedState();
    if (!mobileToggle) return;
    mobileToggle.setAttribute("aria-expanded", String(isOpen));
    mobileToggle.setAttribute("aria-label", isOpen ? "메뉴 닫기" : "메뉴 열기");
  }

  function syncViewport() {
    if (mobileQuery.matches) {
      sidebar.classList.remove("is-collapsed");
      setMobileOpen(false);
      return;
    }
    sidebar.classList.remove("is-open");
    setDesktopCollapsed(readCollapsedPreference(), { persist: false });
  }

  selectGroup(activeGroup);
  syncViewport();

  groupTriggers.forEach((trigger) => {
    trigger.addEventListener("click", () => {
      const groupId = trigger.dataset.navGroupTrigger || activeGroup;
      const isSameGroup = selectedGroup === groupId;
      selectGroup(groupId);

      if (mobileQuery.matches) {
        setMobileOpen(true);
        return;
      }

      if (sidebar.classList.contains("is-collapsed")) {
        setDesktopCollapsed(false);
      } else if (isSameGroup) {
        setDesktopCollapsed(true);
      }
    });
  });

  collapseButton?.addEventListener("click", () => setDesktopCollapsed(true));
  mobileToggle?.addEventListener("click", () => {
    setMobileOpen(!sidebar.classList.contains("is-open"));
  });

  sidebar.querySelectorAll(".nav-link").forEach((link) => {
    link.addEventListener("click", () => {
      if (mobileQuery.matches) setMobileOpen(false);
    });
  });

  document.addEventListener("keydown", (event) => {
    if (event.key !== "Escape") return;
    if (mobileQuery.matches) setMobileOpen(false);
    else setDesktopCollapsed(true);
  });

  mobileQuery.addEventListener("change", syncViewport);
})();
