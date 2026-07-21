(() => {
  const sidebar = document.querySelector(".sidebar-shell");
  const toggle = document.querySelector("[data-nav-toggle]");

  if (!sidebar || !toggle) return;

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
      if (window.matchMedia("(max-width: 980px)").matches) setOpen(false);
    });
  });

  window.addEventListener("resize", () => {
    if (!window.matchMedia("(max-width: 980px)").matches) setOpen(false);
  });
})();
