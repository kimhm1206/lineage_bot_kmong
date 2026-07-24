(() => {
  const modal = document.querySelector("#help-guide-modal");
  if (!modal) return;

  const tabs = [...modal.querySelectorAll("[data-help-guide-tab]")];
  const panels = [...modal.querySelectorAll("[data-help-guide-panel]")];
  const closeButtons = [...modal.querySelectorAll("[data-help-guide-close]")];
  let returnFocus = null;

  function currentPageGuide() {
    return document.querySelector(".nav-link.is-active[data-nav-item]")?.dataset.navItem || "";
  }

  function selectGuide(requestedId, { focus = false } = {}) {
    const target = tabs.some((tab) => tab.dataset.helpGuideTab === requestedId)
      ? requestedId
      : tabs[0]?.dataset.helpGuideTab;
    if (!target) return;

    tabs.forEach((tab) => {
      const selected = tab.dataset.helpGuideTab === target;
      tab.classList.toggle("is-active", selected);
      tab.setAttribute("aria-selected", String(selected));
      tab.tabIndex = selected ? 0 : -1;
      if (selected) {
        tab.scrollIntoView({ block: "nearest", behavior: "auto" });
        if (focus) tab.focus({ preventScroll: true });
      }
    });
    panels.forEach((panel) => {
      const selected = panel.dataset.helpGuidePanel === target;
      panel.hidden = !selected;
      panel.classList.toggle("is-active", selected);
      if (selected) panel.scrollTop = 0;
    });
    modal.dataset.activeGuide = target;
  }

  function openGuide(trigger, requestedGuide = "") {
    returnFocus = trigger instanceof HTMLElement ? trigger : document.activeElement;
    const requested = requestedGuide || trigger?.dataset.guideTarget || currentPageGuide() || modal.dataset.activeGuide;
    selectGuide(requested);
    modal.classList.add("is-open");
    modal.setAttribute("aria-hidden", "false");
    document.body.classList.add("has-open-modal");
    modal.querySelector("[data-help-guide-close].icon-button")?.focus({ preventScroll: true });
  }

  function closeGuide() {
    if (!modal.classList.contains("is-open")) return;
    modal.classList.remove("is-open");
    modal.setAttribute("aria-hidden", "true");
    if (!document.querySelector(".ops-modal.is-open")) {
      document.body.classList.remove("has-open-modal");
    }
    if (returnFocus instanceof HTMLElement && returnFocus.isConnected) {
      returnFocus.focus({ preventScroll: true });
    }
  }

  document.addEventListener("click", (event) => {
    const trigger = event.target.closest("[data-help-guide-open]");
    if (trigger) {
      event.preventDefault();
      openGuide(trigger);
      return;
    }

    const pageLink = event.target.closest(".help-guide-page-link");
    if (pageLink) closeGuide();
  });

  closeButtons.forEach((button) => button.addEventListener("click", closeGuide));

  tabs.forEach((tab, index) => {
    tab.addEventListener("click", () => selectGuide(tab.dataset.helpGuideTab));
    tab.addEventListener("keydown", (event) => {
      if (!["ArrowDown", "ArrowUp", "Home", "End"].includes(event.key)) return;
      event.preventDefault();
      let nextIndex = index;
      if (event.key === "ArrowDown") nextIndex = (index + 1) % tabs.length;
      if (event.key === "ArrowUp") nextIndex = (index - 1 + tabs.length) % tabs.length;
      if (event.key === "Home") nextIndex = 0;
      if (event.key === "End") nextIndex = tabs.length - 1;
      selectGuide(tabs[nextIndex]?.dataset.helpGuideTab, { focus: true });
    });
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && modal.classList.contains("is-open")) closeGuide();
  });

  selectGuide(modal.dataset.activeGuide);
  const directGuide = new URL(window.location.href).searchParams.get("help");
  if (directGuide) {
    window.requestAnimationFrame(() => openGuide(null, directGuide));
  }
})();
