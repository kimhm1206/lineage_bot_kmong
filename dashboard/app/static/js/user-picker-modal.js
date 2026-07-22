(() => {
  class UserPickerModal {
    constructor(root) {
      if (!root) throw new Error("UserPickerModal root element is required.");
      this.root = root;
      this.dialog = root.querySelector(".user-picker-dialog");
      this.title = root.querySelector("[data-user-picker-title]");
      this.search = root.querySelector("[data-user-picker-search]");
      this.modeLabel = root.querySelector("[data-user-picker-mode]");
      this.selectionCount = root.querySelector("[data-user-picker-count]");
      this.resultCount = root.querySelector("[data-user-picker-result-count]");
      this.results = root.querySelector("[data-user-picker-results]");
      this.confirmButton = root.querySelector("[data-user-picker-confirm]");
      this.cancelButtons = root.querySelectorAll("[data-user-picker-close], [data-user-picker-cancel]");
      this.users = [];
      this.selectedIds = new Set();
      this.excludedIds = new Set();
      this.multiple = false;
      this.resolve = null;
      this.previouslyFocused = null;

      this.search.addEventListener("input", () => this.render());
      this.confirmButton.addEventListener("click", () => this.confirm());
      this.cancelButtons.forEach((button) => button.addEventListener("click", () => this.cancel()));
      this.root.addEventListener("click", (event) => {
        if (event.target === this.root) this.cancel();
      });
      document.addEventListener("keydown", (event) => {
        if (event.key === "Escape" && !this.root.classList.contains("is-hidden")) this.cancel();
      });
    }

    /** Opens the shared picker. Set multiple=true when a workflow accepts several users. */
    open(options = {}) {
      if (this.resolve) this.cancel();
      this.users = Array.isArray(options.users) ? options.users : [];
      this.multiple = Boolean(options.multiple);
      this.excludedIds = new Set((options.excludedIds || []).map(String));
      const selectedIds = (options.selectedIds || [])
        .map(String)
        .filter((id) => !this.excludedIds.has(id));
      this.selectedIds = new Set(this.multiple ? selectedIds : selectedIds.slice(0, 1));
      this.title.textContent = options.title || (this.multiple ? "유저 여러 명 선택" : "유저 선택");
      this.modeLabel.textContent = this.multiple ? "여러 명 선택 가능" : "한 명 선택";
      this.search.value = "";
      this.previouslyFocused = document.activeElement;
      this.root.classList.remove("is-hidden");
      this.root.setAttribute("aria-hidden", "false");
      document.body.classList.add("modal-open");
      this.render();
      window.requestAnimationFrame(() => this.search.focus());

      return new Promise((resolve) => {
        this.resolve = resolve;
      });
    }

    close(result) {
      this.root.classList.add("is-hidden");
      this.root.setAttribute("aria-hidden", "true");
      document.body.classList.remove("modal-open");
      const resolve = this.resolve;
      this.resolve = null;
      if (this.previouslyFocused instanceof HTMLElement) this.previouslyFocused.focus();
      if (resolve) resolve(result);
    }

    cancel() {
      this.close(null);
    }

    confirm() {
      const selectedUsers = this.users.filter((user) => this.selectedIds.has(String(user.discord_id)));
      if (!selectedUsers.length) return;
      this.close(selectedUsers);
    }

    toggle(user) {
      const id = String(user.discord_id);
      if (this.excludedIds.has(id)) return;
      if (!this.multiple) {
        this.selectedIds.clear();
        this.selectedIds.add(id);
      } else if (this.selectedIds.has(id)) {
        this.selectedIds.delete(id);
      } else {
        this.selectedIds.add(id);
      }
      this.render();
    }

    normalized(value) {
      return String(value || "").trim().toLocaleLowerCase("ko-KR");
    }

    matches(user, query) {
      if (!query) return true;
      return this.normalized(`${user.display_name} ${user.username || ""} ${user.discord_id}`).includes(query);
    }

    createUserRow(user) {
      const id = String(user.discord_id);
      const isSelected = this.selectedIds.has(id);
      const isExcluded = this.excludedIds.has(id);
      const button = document.createElement("button");
      button.type = "button";
      button.className = "user-picker-row";
      button.classList.toggle("is-selected", isSelected);
      button.classList.toggle("is-disabled", isExcluded);
      button.disabled = isExcluded;
      button.setAttribute("aria-pressed", String(isSelected));

      const copy = document.createElement("span");
      copy.className = "user-picker-row-copy";
      const name = document.createElement("strong");
      name.textContent = user.display_name;
      const detail = document.createElement("small");
      detail.textContent = user.username ? `${user.username} · ${user.discord_id}` : String(user.discord_id);
      copy.append(name, detail);

      const state = document.createElement("span");
      state.className = "user-picker-row-state";
      state.textContent = isExcluded ? "이미 지정됨" : (isSelected ? "선택됨" : "선택");
      button.append(copy, state);
      button.addEventListener("click", () => this.toggle(user));
      return button;
    }

    render() {
      const query = this.normalized(this.search.value);
      const matched = this.users.filter((user) => this.matches(user, query));
      const visible = matched.slice(0, 100);
      this.results.replaceChildren();

      if (!visible.length) {
        const empty = document.createElement("div");
        empty.className = "user-picker-empty";
        empty.textContent = query ? "검색 결과가 없습니다." : "선택할 수 있는 서버 유저가 없습니다.";
        this.results.append(empty);
      } else {
        visible.forEach((user) => this.results.append(this.createUserRow(user)));
      }

      const selectedCount = this.selectedIds.size;
      this.selectionCount.textContent = `${selectedCount}명 선택`;
      this.resultCount.textContent = matched.length > visible.length
        ? `${visible.length}명 표시 · 검색 결과 ${matched.length}명`
        : `${matched.length}명`;
      this.confirmButton.disabled = selectedCount === 0;
    }
  }

  document.addEventListener("DOMContentLoaded", () => {
    const root = document.querySelector("[data-user-picker-modal]");
    if (!root) return;
    window.dashboardUserPicker = new UserPickerModal(root);
  });
  window.UserPickerModal = UserPickerModal;
})();
