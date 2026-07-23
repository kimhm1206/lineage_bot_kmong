(() => {
  const body = document.body;

  const showToast = (message, tone = "success") => {
    let region = document.querySelector(".ops-toast-region");
    if (!region) {
      region = document.createElement("div");
      region.className = "ops-toast-region";
      document.body.append(region);
    }
    const toast = document.createElement("div");
    toast.className = `ops-toast ops-toast-${tone}`;
    toast.textContent = message;
    region.append(toast);
    requestAnimationFrame(() => toast.classList.add("is-visible"));
    window.setTimeout(() => {
      toast.classList.remove("is-visible");
      window.setTimeout(() => toast.remove(), 180);
    }, 3200);
  };

  const openModal = (id) => {
    const modal = document.getElementById(id);
    if (!modal) return;
    modal.classList.add("is-open");
    modal.setAttribute("aria-hidden", "false");
    body.classList.add("has-open-modal");
    window.setTimeout(() => modal.querySelector("input:not([type=hidden]), select")?.focus(), 30);
  };

  const closeModal = (modal) => {
    const target = modal?.closest?.(".ops-modal") || modal;
    if (!target) return;
    target.classList.remove("is-open");
    target.setAttribute("aria-hidden", "true");
    if (!document.querySelector(".ops-modal.is-open")) body.classList.remove("has-open-modal");
  };

  const preservedDetails = () =>
    [...document.querySelectorAll("details[open][data-detail-key]")].map((detail) => detail.dataset.detailKey);

  const normalizeSearch = (value) =>
    String(value || "")
      .normalize("NFKC")
      .toLocaleLowerCase("ko-KR")
      .replace(/\s+/g, " ")
      .trim();

  const applyClientSearch = (input) => {
    if (!input) return;
    const page = input.closest("[data-live-page]");
    if (!page) return;
    const query = normalizeSearch(input.value);
    const items = [...page.querySelectorAll("[data-client-search-item]")];
    let visibleCount = 0;
    items.forEach((item) => {
      const source = item.dataset.searchText || item.textContent;
      const isVisible = !query || normalizeSearch(source).includes(query);
      item.hidden = !isVisible;
      if (isVisible) visibleCount += 1;
    });
    page.querySelectorAll("[data-client-search-count]").forEach((counter) => {
      counter.textContent = `${visibleCount}${counter.dataset.countSuffix || "개"}`;
    });
    const empty = page.querySelector("[data-client-search-empty]");
    if (empty) empty.hidden = !query || visibleCount > 0;
  };

  const refreshLivePage = async () => {
    const current = document.querySelector("[data-live-page]");
    if (!current) return;
    const openKeys = preservedDetails();
    const clientSearchValue = current.querySelector("[data-client-search-input]")?.value || "";
    const response = await fetch(window.location.href, { headers: { "X-Partial-Page": "1" } });
    if (!response.ok) throw new Error("화면 갱신에 실패했습니다.");
    const html = await response.text();
    const parsed = new DOMParser().parseFromString(html, "text/html");
    const next = parsed.querySelector("[data-live-page]");
    if (!next) throw new Error("갱신할 화면을 찾지 못했습니다.");
    current.innerHTML = next.innerHTML;
    openKeys.forEach((key) => {
      const detail = [...current.querySelectorAll("details[data-detail-key]")].find(
        (candidate) => candidate.dataset.detailKey === key,
      );
      if (detail) detail.open = true;
    });
    const clientSearchInput = current.querySelector("[data-client-search-input]");
    if (clientSearchInput) {
      clientSearchInput.value = clientSearchValue;
      applyClientSearch(clientSearchInput);
    }
  };

  const asyncSubmit = async (form) => {
    const confirmation = form.dataset.confirm;
    if (confirmation && !window.confirm(confirmation)) return;
    const submitter = form.querySelector("button[type=submit]");
    const originalHtml = submitter?.innerHTML;
    if (submitter) {
      submitter.disabled = true;
      submitter.textContent = "처리 중";
    }
    try {
      const response = await fetch(form.action, {
        method: (form.method || "POST").toUpperCase(),
        body: new FormData(form),
        headers: { Accept: "application/json" },
      });
      const payload = await response.json().catch(() => ({ ok: false, message: "서버 응답을 확인하지 못했습니다." }));
      if (!response.ok || !payload.ok) throw new Error(payload.message || "작업을 완료하지 못했습니다.");
      closeModal(form);
      showToast(payload.message || "처리했습니다.");
      await refreshLivePage();
    } catch (error) {
      showToast(error.message || "작업 중 오류가 발생했습니다.", "error");
    } finally {
      if (submitter) {
        submitter.disabled = false;
        submitter.innerHTML = originalHtml;
      }
    }
  };

  const setItemEditor = (row, isOpen) => {
    if (!row) return;
    const editor = row.querySelector("[data-item-editor]");
    const toggle = row.querySelector("[data-item-edit-toggle]");
    if (!editor || !toggle) return;
    editor.hidden = !isOpen;
    row.classList.toggle("is-editing", isOpen);
    toggle.setAttribute("aria-expanded", String(isOpen));
    if (isOpen) window.setTimeout(() => editor.elements.item_name?.focus(), 20);
  };

  const filterBuyerUsers = (form) => {
    const allianceId = form.querySelector("[data-buyer-alliance]")?.value || "";
    const userSelect = form.querySelector("[data-buyer-user]");
    if (!userSelect) return;
    [...userSelect.options].forEach((option, index) => {
      if (index === 0) return;
      option.hidden = option.dataset.allianceId !== allianceId;
      option.disabled = option.hidden;
    });
    if (userSelect.selectedOptions[0]?.disabled) userSelect.value = "";
  };

  const updateSalePreview = (form) => {
    const cash = Number(form.dataset.cashPrice || 0);
    const rate = Number(form.querySelector("[data-sale-rate]")?.value || 0);
    const adena = rate > 0 ? Math.floor((cash * 10000) / rate) : 0;
    const output = form.closest(".ops-modal")?.querySelector("[data-sale-adena]");
    if (output) output.textContent = `${adena.toLocaleString("ko-KR")} 아데나`;
  };

  const populateDropEdit = (button) => {
    const drop = JSON.parse(button.dataset.drop);
    const form = document.querySelector("[data-drop-edit-form]");
    form.action = `/api/drops/${drop.drop_id}`;
    form.elements.attendance_id.value = String(drop.attendance_id);
    form.elements.item_id.value = String(drop.item_id);
    const excluded = new Set((drop.excluded_alliance_ids || []).map(String));
    form.querySelectorAll('input[name="excluded_alliance_ids"]').forEach((checkbox) => {
      checkbox.checked = excluded.has(checkbox.value);
    });
    openModal("drop-edit-modal");
  };

  const populateSale = (button) => {
    const drop = JSON.parse(button.dataset.drop);
    const modal = document.getElementById("drop-sale-modal");
    const form = modal.querySelector("[data-sale-form]");
    form.action = `/api/drops/${drop.drop_id}/sale`;
    form.dataset.cashPrice = String(Number(drop.cash_price_krw || 0));
    form.elements.buyer_alliance_id.value = drop.buyer_alliance_id ? String(drop.buyer_alliance_id) : "";
    form.elements.adena_market_rate.value = Number(drop.adena_market_rate || 0) > 1 ? drop.adena_market_rate : "";
    modal.querySelector("[data-sale-item]").textContent = `${drop.item_name} · 출석 #${drop.attendance_id}`;
    modal.querySelector("[data-sale-cash-label]").textContent = `${Number(drop.cash_price_krw || 0).toLocaleString("ko-KR")}원`;
    filterBuyerUsers(form);
    form.elements.buyer_user_id.value = drop.buyer_user_id ? String(drop.buyer_user_id) : "";
    updateSalePreview(form);
    openModal("drop-sale-modal");
  };

  const populateFeeEdit = (button) => {
    const fee = JSON.parse(button.dataset.fee);
    const form = document.querySelector("[data-fee-edit-form]");
    form.action = `/api/fee-rules/${fee.fee_rule_id}`;
    form.elements.rule_name.value = fee.rule_name;
    form.elements.percent.value = Number(fee.rate_ppm || 0) / 10000;
    form.elements.is_active.checked = Boolean(fee.is_active);
    const isFixed = Boolean(fee.is_fixed);
    form.elements.rule_name.readOnly = isFixed;
    form.elements.is_active.disabled = isFixed;
    const help = form.querySelector("[data-fee-rule-help]");
    if (help) {
      help.textContent = isFixed
        ? "가계부 자동 반영 항목은 항상 사용되며 이름을 변경할 수 없습니다."
        : "끄면 새 정산 계산에서 제외됩니다.";
    }
    openModal("fee-edit-modal");
  };

  let bidHistoryRequest = 0;

  const openBidHistory = async (button) => {
    const modal = document.getElementById("bid-history-modal");
    if (!modal) return;
    const requestNumber = ++bidHistoryRequest;
    const itemName = button.dataset.itemName || "아이템";
    const itemId = button.dataset.itemId;
    const guildId = button.dataset.guildId;
    const itemOutput = modal.querySelector("[data-bid-history-item]");
    const loading = modal.querySelector("[data-bid-history-loading]");
    const list = modal.querySelector("[data-bid-history-list]");
    const empty = modal.querySelector("[data-bid-history-empty]");

    itemOutput.textContent = itemName;
    loading.hidden = false;
    loading.textContent = "구매 기록을 불러오는 중입니다.";
    list.hidden = true;
    list.replaceChildren();
    empty.hidden = true;
    openModal("bid-history-modal");

    try {
      const response = await fetch(
        `/api/bid-purchases/items/${encodeURIComponent(itemId)}?guild_id=${encodeURIComponent(guildId)}`,
        { headers: { Accept: "application/json" } },
      );
      const payload = await response.json().catch(() => ({}));
      if (!response.ok || !payload.ok) {
        throw new Error(payload.detail || payload.message || "구매 기록을 불러오지 못했습니다.");
      }
      if (requestNumber !== bidHistoryRequest) return;
      itemOutput.textContent = payload.item_name || itemName;
      loading.hidden = true;
      const history = Array.isArray(payload.history) ? payload.history : [];
      if (!history.length) {
        empty.hidden = false;
        return;
      }
      const fragment = document.createDocumentFragment();
      history.forEach((record, index) => {
        const row = document.createElement("article");
        row.className = "bid-history-row";
        const number = document.createElement("span");
        number.className = "bid-history-number";
        number.textContent = String(index + 1);
        const copy = document.createElement("div");
        const alliance = document.createElement("strong");
        alliance.textContent = record.alliance_name || "알 수 없는 혈맹";
        const date = document.createElement("small");
        date.textContent = record.purchased_at_short || record.purchased_at || "-";
        copy.append(alliance, date);
        row.append(number, copy);
        fragment.append(row);
      });
      list.append(fragment);
      list.hidden = false;
    } catch (error) {
      if (requestNumber !== bidHistoryRequest) return;
      loading.hidden = false;
      loading.textContent = error.message || "구매 기록을 불러오지 못했습니다.";
    }
  };

  const clanHistoryState = {
    page: 1,
    status: "all",
    query: "",
    request: 0,
    loading: false,
    hasNext: false,
  };
  let clanHistorySearchTimer = 0;

  const renderClanHistoryRow = (record) => {
    const row = document.createElement("article");
    row.className = `settlement-history-row status-${record.status_tone || "complete"}`;

    const target = document.createElement("div");
    target.className = "settlement-history-target";
    const targetType = document.createElement("small");
    targetType.textContent = record.target_type || "정산 대상";
    const targetName = document.createElement("strong");
    targetName.textContent = record.target_name || "알 수 없는 대상";
    target.append(targetType, targetName);

    const item = document.createElement("div");
    item.className = "settlement-history-item";
    const itemName = document.createElement("strong");
    itemName.textContent = record.item_name || "이름 없는 아이템";
    const attendance = document.createElement("small");
    attendance.textContent = `출석 #${record.attendance_id || "-"} · ${record.occurred_at_label || "-"}`;
    item.append(itemName, attendance);

    const amount = document.createElement("div");
    amount.className = "settlement-history-amount";
    const amountValue = document.createElement("strong");
    amountValue.textContent = Number(record.amount_adena || 0).toLocaleString("ko-KR");
    const amountUnit = document.createElement("small");
    amountUnit.textContent = "아데나";
    amount.append(amountValue, amountUnit);

    const result = document.createElement("div");
    result.className = "settlement-history-result";
    const status = document.createElement("span");
    status.className = `ops-status ops-status-${record.status_tone || "complete"}`;
    status.textContent = record.status_label || "완료";
    const completedAt = document.createElement("small");
    completedAt.textContent = record.completed_at_label || "-";
    result.append(status, completedAt);

    row.append(target, item, amount, result);
    return row;
  };

  const loadClanHistory = async ({ reset = false } = {}) => {
    const modal = document.querySelector("[data-clan-history-modal]");
    if (!modal || (clanHistoryState.loading && !reset)) return;
    if (reset && clanHistoryState.loading) {
      clanHistoryState.request += 1;
      clanHistoryState.loading = false;
    }
    if (reset) {
      clanHistoryState.page = 1;
      clanHistoryState.hasNext = false;
    }
    const requestNumber = ++clanHistoryState.request;
    const list = modal.querySelector("[data-history-list]");
    const loading = modal.querySelector("[data-history-loading]");
    const empty = modal.querySelector("[data-history-empty]");
    const more = modal.querySelector("[data-history-more]");
    clanHistoryState.loading = true;
    loading.hidden = false;
    loading.textContent = "과거 기록을 불러오는 중입니다.";
    empty.hidden = true;
    more.hidden = true;
    if (reset) {
      list.hidden = true;
      list.replaceChildren();
    }

    const params = new URLSearchParams({
      guild_id: modal.dataset.guildId,
      alliance_id: modal.dataset.allianceId,
      period: modal.dataset.period || "30",
      history_status: clanHistoryState.status,
      page: String(clanHistoryState.page),
    });
    if (clanHistoryState.query) params.set("q", clanHistoryState.query);

    try {
      const response = await fetch(`/api/clan-settlement-history?${params.toString()}`, {
        headers: { Accept: "application/json" },
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok || !payload.ok) {
        throw new Error(payload.detail || payload.message || "과거 기록을 불러오지 못했습니다.");
      }
      if (requestNumber !== clanHistoryState.request) return;
      modal.querySelector("[data-history-complete-count]").textContent =
        `${Number(payload.summary?.complete_count || 0).toLocaleString("ko-KR")}건`;
      modal.querySelector("[data-history-forfeited-count]").textContent =
        `${Number(payload.summary?.forfeited_count || 0).toLocaleString("ko-KR")}건`;
      modal.querySelector("[data-history-total-amount]").textContent =
        Number(String(payload.summary?.total_amount_label || "0").replaceAll(",", "")).toLocaleString("ko-KR");

      const records = Array.isArray(payload.history) ? payload.history : [];
      const fragment = document.createDocumentFragment();
      records.forEach((record) => fragment.append(renderClanHistoryRow(record)));
      list.append(fragment);
      list.hidden = list.children.length === 0;
      empty.hidden = list.children.length > 0;
      clanHistoryState.hasNext = Boolean(payload.pagination?.has_next);
      more.hidden = !clanHistoryState.hasNext;
      loading.hidden = true;
    } catch (error) {
      if (requestNumber !== clanHistoryState.request) return;
      loading.hidden = false;
      loading.textContent = error.message || "과거 기록을 불러오지 못했습니다.";
    } finally {
      if (requestNumber === clanHistoryState.request) clanHistoryState.loading = false;
    }
  };

  const openClanHistory = (button) => {
    const modal = document.querySelector("[data-clan-history-modal]");
    if (!modal) return;
    modal.dataset.guildId = button.dataset.guildId || modal.dataset.guildId;
    modal.dataset.allianceId = button.dataset.allianceId || modal.dataset.allianceId;
    modal.dataset.period = button.dataset.period || modal.dataset.period;
    clanHistoryState.status = "all";
    clanHistoryState.query = "";
    modal.querySelector("[data-history-search]").value = "";
    modal.querySelectorAll("[data-history-status]").forEach((filter) => {
      filter.classList.toggle("is-active", filter.dataset.historyStatus === "all");
    });
    openModal("clan-settlement-history-modal");
    loadClanHistory({ reset: true });
  };

  document.addEventListener("submit", (event) => {
    const clientSearchForm = event.target.closest("[data-client-search-form]");
    if (clientSearchForm) {
      event.preventDefault();
      event.stopPropagation();
      applyClientSearch(clientSearchForm.querySelector("[data-client-search-input]"));
      return;
    }
    const form = event.target.closest("[data-async-form]");
    if (!form) return;
    event.preventDefault();
    event.stopPropagation();
    asyncSubmit(form);
  });

  document.addEventListener("click", (event) => {
    if (event.target.closest("summary form, summary button")) event.stopPropagation();
    const modalOpen = event.target.closest("[data-modal-open]");
    if (modalOpen) openModal(modalOpen.dataset.modalOpen);
    const modalClose = event.target.closest("[data-modal-close]");
    if (modalClose) closeModal(modalClose);
    const dropEdit = event.target.closest("[data-drop-edit]");
    if (dropEdit) populateDropEdit(dropEdit);
    const saleOpen = event.target.closest("[data-sale-open]");
    if (saleOpen) populateSale(saleOpen);
    const itemEditToggle = event.target.closest("[data-item-edit-toggle]");
    if (itemEditToggle) {
      const row = itemEditToggle.closest("[data-item-row]");
      const nextState = row?.querySelector("[data-item-editor]")?.hidden ?? false;
      document.querySelectorAll("[data-item-row].is-editing").forEach((openRow) => {
        if (openRow !== row) setItemEditor(openRow, false);
      });
      setItemEditor(row, nextState);
    }
    const itemEditCancel = event.target.closest("[data-item-edit-cancel]");
    if (itemEditCancel) setItemEditor(itemEditCancel.closest("[data-item-row]"), false);
    const feeEdit = event.target.closest("[data-fee-edit]");
    if (feeEdit) populateFeeEdit(feeEdit);
    const bidHistoryOpen = event.target.closest("[data-bid-history-open]");
    if (bidHistoryOpen) openBidHistory(bidHistoryOpen);
    const clanHistoryOpen = event.target.closest("[data-clan-history-open]");
    if (clanHistoryOpen) openClanHistory(clanHistoryOpen);
    const clanHistoryStatus = event.target.closest("[data-history-status]");
    if (clanHistoryStatus) {
      clanHistoryState.status = clanHistoryStatus.dataset.historyStatus || "all";
      clanHistoryStatus.parentElement.querySelectorAll("[data-history-status]").forEach((filter) => {
        filter.classList.toggle("is-active", filter === clanHistoryStatus);
      });
      loadClanHistory({ reset: true });
    }
    const clanHistoryMore = event.target.closest("[data-history-more]");
    if (clanHistoryMore && clanHistoryState.hasNext) {
      clanHistoryState.page += 1;
      loadClanHistory();
    }

    const statusFilter = event.target.closest("[data-personal-filter] button");
    if (statusFilter) {
      const filter = statusFilter.dataset.status;
      statusFilter.parentElement.querySelectorAll("button").forEach((button) => button.classList.toggle("is-active", button === statusFilter));
      document.querySelectorAll("[data-personal-status]").forEach((card) => {
        card.hidden = filter !== "all" && card.dataset.personalStatus !== filter;
      });
    }
  });

  document.addEventListener("change", (event) => {
    const buyerAlliance = event.target.closest("[data-buyer-alliance]");
    if (buyerAlliance) filterBuyerUsers(buyerAlliance.form);
  });

  document.addEventListener("input", (event) => {
    const clientSearchInput = event.target.closest("[data-client-search-input]");
    if (clientSearchInput) applyClientSearch(clientSearchInput);
    const saleInput = event.target.closest("[data-sale-rate]");
    if (saleInput) updateSalePreview(saleInput.form);
    const historySearch = event.target.closest("[data-history-search]");
    if (historySearch) {
      window.clearTimeout(clanHistorySearchTimer);
      clanHistorySearchTimer = window.setTimeout(() => {
        clanHistoryState.query = historySearch.value.trim();
        loadClanHistory({ reset: true });
      }, 280);
    }
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") closeModal(document.querySelector(".ops-modal.is-open"));
  });

  const initializeOperationsPage = () => {
    document.querySelectorAll("[data-client-search-input]").forEach(applyClientSearch);
  };

  document.addEventListener("dashboard:page-loaded", initializeOperationsPage);
  initializeOperationsPage();
})();
