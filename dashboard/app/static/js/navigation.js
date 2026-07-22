(() => {
  const sidebar = document.querySelector(".sidebar-shell");
  const toggle = document.querySelector("[data-nav-toggle]");
  const sidebarScrollKey = "lineage-dashboard:sidebar-scroll-top";

  if (!sidebar || !toggle) return;

  function readSidebarScroll() {
    try {
      const value = Number.parseInt(sessionStorage.getItem(sidebarScrollKey) || "0", 10);
      return Number.isFinite(value) ? Math.max(value, 0) : 0;
    } catch {
      return 0;
    }
  }

  function saveSidebarScroll() {
    try {
      sessionStorage.setItem(sidebarScrollKey, String(Math.round(sidebar.scrollTop)));
    } catch {
      // The sidebar still works when browser storage is unavailable.
    }
  }

  function restoreSidebarScroll() {
    const savedScroll = readSidebarScroll();
    const maxScroll = Math.max(sidebar.scrollHeight - sidebar.clientHeight, 0);
    sidebar.scrollTop = Math.min(savedScroll, maxScroll);
  }

  restoreSidebarScroll();
  window.requestAnimationFrame(restoreSidebarScroll);
  window.addEventListener("load", restoreSidebarScroll, { once: true });

  let scrollFrame = null;
  sidebar.addEventListener("scroll", () => {
    if (scrollFrame !== null) return;
    scrollFrame = window.requestAnimationFrame(() => {
      saveSidebarScroll();
      scrollFrame = null;
    });
  }, { passive: true });

  window.addEventListener("pagehide", saveSidebarScroll);

  function setOpen(isOpen) {
    sidebar.classList.toggle("is-open", isOpen);
    toggle.setAttribute("aria-expanded", String(isOpen));
    toggle.setAttribute("aria-label", isOpen ? "메뉴 닫기" : "메뉴 열기");
  }

  toggle.addEventListener("click", () => {
    setOpen(!sidebar.classList.contains("is-open"));
  });

  sidebar.querySelectorAll(".nav-link").forEach((link) => {
    link.addEventListener("click", () => {
      saveSidebarScroll();
      if (window.matchMedia("(max-width: 980px)").matches) setOpen(false);
    });
  });

  window.addEventListener("resize", () => {
    if (!window.matchMedia("(max-width: 980px)").matches) setOpen(false);
  });
})();
