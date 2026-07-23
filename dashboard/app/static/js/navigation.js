(() => {
  const sidebar = document.querySelector("[data-sidebar]");
  const contentShell = document.querySelector(".content-shell");
  if (!sidebar || !contentShell) return;

  const groupTriggers = [...sidebar.querySelectorAll("[data-nav-group-trigger]")];
  const groupPanels = [...sidebar.querySelectorAll("[data-nav-group-panel]")];
  const navLinks = [...sidebar.querySelectorAll(".nav-link")];
  const mobileToggle = sidebar.querySelector("[data-nav-toggle]");
  const collapseButton = sidebar.querySelector("[data-sidebar-collapse]");
  const mobileQuery = window.matchMedia("(max-width: 980px)");
  const collapsedKey = "lineage-dashboard:sidebar-collapsed";
  const activeGroup = sidebar.dataset.activeGroup || groupTriggers[0]?.dataset.navGroupTrigger || "";
  let selectedGroup = activeGroup;
  let navigationController = null;

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

  function updateExpandedState() {
    const panelIsVisible = mobileQuery.matches
      ? sidebar.classList.contains("is-open")
      : !sidebar.classList.contains("is-collapsed");
    groupTriggers.forEach((trigger) => {
      const isSelected = trigger.dataset.navGroupTrigger === selectedGroup;
      trigger.setAttribute("aria-expanded", String(isSelected && panelIsVisible));
    });
  }

  function selectGroup(groupId) {
    if (!groupId) return;
    selectedGroup = groupId;
    groupTriggers.forEach((trigger) => {
      trigger.classList.toggle("is-active", trigger.dataset.navGroupTrigger === groupId);
    });
    groupPanels.forEach((panel) => {
      const isSelected = panel.dataset.navGroupPanel === groupId;
      panel.hidden = !isSelected;
      panel.classList.toggle("is-current", isSelected);
    });
    updateExpandedState();
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

  function pathKey(value) {
    const url = new URL(value, window.location.href);
    return `${url.pathname}${url.search}`;
  }

  function syncActiveLink(url) {
    const targetPath = new URL(url, window.location.href).pathname;
    let activeLink = null;
    navLinks.forEach((link) => {
      const isActive = new URL(link.href).pathname === targetPath;
      link.classList.toggle("is-active", isActive);
      if (isActive) {
        link.setAttribute("aria-current", "page");
        activeLink = link;
      } else {
        link.removeAttribute("aria-current");
      }
    });
    const groupId = activeLink?.closest("[data-nav-group-panel]")?.dataset.navGroupPanel;
    if (groupId) selectGroup(groupId);
  }

  function updateImmediateTitle(title) {
    if (!title) return;
    const topbarTitle = document.querySelector(".topbar-context strong");
    if (topbarTitle) topbarTitle.textContent = title;
    const suffix = document.title.includes("·") ? document.title.split("·").slice(1).join("·").trim() : "";
    document.title = suffix ? `${title} · ${suffix}` : title;
  }

  function showLoading(title) {
    updateImmediateTitle(title);
    contentShell.setAttribute("aria-busy", "true");
    const loading = document.createElement("section");
    loading.className = "page-loading-state";
    loading.setAttribute("role", "status");
    loading.setAttribute("aria-live", "polite");

    const spinner = document.createElement("span");
    spinner.className = "page-loading-spinner";
    spinner.setAttribute("aria-hidden", "true");
    const copy = document.createElement("div");
    const heading = document.createElement("strong");
    heading.textContent = title ? `${title} 불러오는 중` : "페이지 불러오는 중";
    const description = document.createElement("p");
    description.textContent = "필요한 데이터를 준비하고 있습니다.";
    copy.append(heading, description);

    const skeleton = document.createElement("div");
    skeleton.className = "page-loading-skeleton";
    skeleton.setAttribute("aria-hidden", "true");
    for (let index = 0; index < 3; index += 1) {
      const line = document.createElement("i");
      skeleton.append(line);
    }
    loading.append(spinner, copy, skeleton);
    contentShell.replaceChildren(loading);
  }

  function showNavigationError(message, retry) {
    contentShell.removeAttribute("aria-busy");
    const error = document.createElement("section");
    error.className = "page-navigation-error";
    const title = document.createElement("strong");
    title.textContent = "페이지를 불러오지 못했습니다.";
    const description = document.createElement("p");
    description.textContent = message || "잠시 후 다시 시도해 주세요.";
    const button = document.createElement("button");
    button.type = "button";
    button.className = "primary-button";
    button.textContent = "다시 불러오기";
    button.addEventListener("click", retry);
    error.append(title, description, button);
    contentShell.replaceChildren(error);
  }

  function applyFetchedPage(parsed, url) {
    const nextContent = parsed.querySelector(".content-shell");
    const nextTopbarContext = parsed.querySelector(".topbar-context");
    if (!nextContent) throw new Error("페이지 본문을 찾지 못했습니다.");

    contentShell.innerHTML = nextContent.innerHTML;
    contentShell.removeAttribute("aria-busy");
    const currentTopbarContext = document.querySelector(".topbar-context");
    if (currentTopbarContext && nextTopbarContext) {
      currentTopbarContext.innerHTML = nextTopbarContext.innerHTML;
    }
    document.title = parsed.title || document.title;
    syncActiveLink(url);
    document.dispatchEvent(new CustomEvent("dashboard:page-loaded", { detail: { url } }));
    window.scrollTo({ top: 0, behavior: "auto" });
  }

  async function navigate(url, { push = true, title = "" } = {}) {
    const target = new URL(url, window.location.href);
    if (target.origin !== window.location.origin) {
      window.location.href = target.href;
      return;
    }

    navigationController?.abort();
    navigationController = new AbortController();
    if (push && pathKey(target.href) !== pathKey(window.location.href)) {
      window.history.pushState({ dashboardNavigation: true }, "", target.href);
    }
    syncActiveLink(target.href);
    if (mobileQuery.matches) setMobileOpen(false);
    showLoading(title);

    try {
      const response = await fetch(target.href, {
        signal: navigationController.signal,
        headers: { "X-Dashboard-Navigation": "1" },
      });
      if (!response.ok) throw new Error(`서버 응답 ${response.status}`);
      const html = await response.text();
      const parsed = new DOMParser().parseFromString(html, "text/html");
      const finalUrl = new URL(response.url || target.href, window.location.href);
      if (pathKey(finalUrl.href) !== pathKey(window.location.href)) {
        window.history.replaceState({ dashboardNavigation: true }, "", finalUrl.href);
      }
      applyFetchedPage(parsed, finalUrl.href);
    } catch (error) {
      if (error.name === "AbortError") return;
      showNavigationError(error.message, () => navigate(window.location.href, { push: false, title }));
    }
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

  navLinks.forEach((link) => {
    link.addEventListener("click", (event) => {
      if (
        event.defaultPrevented
        || event.button !== 0
        || event.metaKey
        || event.ctrlKey
        || event.shiftKey
        || event.altKey
        || link.target === "_blank"
      ) return;
      event.preventDefault();
      const title = link.querySelector("strong")?.textContent?.trim() || "";
      navigate(link.href, { title });
    });
  });

  window.addEventListener("popstate", () => {
    const matching = navLinks.find((link) => new URL(link.href).pathname === window.location.pathname);
    const title = matching?.querySelector("strong")?.textContent?.trim() || "";
    navigate(window.location.href, { push: false, title });
  });

  document.addEventListener("keydown", (event) => {
    if (event.key !== "Escape") return;
    if (mobileQuery.matches) setMobileOpen(false);
    else setDesktopCollapsed(true);
  });

  mobileQuery.addEventListener("change", syncViewport);
})();
