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

  const appendHistoryDateParams = (params, state) => {
    if (state.period !== "-1") return;
    params.set("date_from", state.dateFrom || "");
    params.set("date_to", state.dateTo || "");
  };

  const resetHistoryPeriodButtons = (selector, datasetKey) => {
    document.querySelectorAll(selector).forEach((button) => {
      button.classList.toggle("is-active", button.dataset[datasetKey] === "30");
    });
  };

  let historyDateApply = null;

  const openHistoryDateRange = (state, apply) => {
    const modal = document.getElementById("history-date-range-modal");
    if (!modal) return;
    const today = new Date();
    const monthAgo = new Date(today);
    monthAgo.setDate(today.getDate() - 29);
    const iso = (value) => {
      const local = new Date(value.getTime() - value.getTimezoneOffset() * 60000);
      return local.toISOString().slice(0, 10);
    };
    modal.querySelector("[data-history-date-from]").value =
      state.dateFrom || iso(monthAgo);
    modal.querySelector("[data-history-date-to]").value =
      state.dateTo || iso(today);
    modal.querySelector("[data-history-date-error]").hidden = true;
    historyDateApply = apply;
    openModal("history-date-range-modal");
  };

  const selectHistoryPeriod = ({
    button,
    state,
    datasetKey,
    buttonSelector,
    load,
  }) => {
    const selected = button.dataset[datasetKey] || "30";
    if (selected === "custom") {
      openHistoryDateRange(state, ({ dateFrom, dateTo }) => {
        state.period = "-1";
        state.dateFrom = dateFrom;
        state.dateTo = dateTo;
        document.querySelectorAll(buttonSelector).forEach((candidate) => {
          candidate.classList.toggle("is-active", candidate === button);
        });
        load({ reset: true });
      });
      return;
    }
    state.period = selected;
    state.dateFrom = "";
    state.dateTo = "";
    document.querySelectorAll(buttonSelector).forEach((candidate) => {
      candidate.classList.toggle("is-active", candidate === button);
    });
    load({ reset: true });
  };

  let pendingConfirmationForm = null;

  const closeConfirmation = () => {
    pendingConfirmationForm = null;
    closeModal(document.getElementById("ops-confirm-modal"));
  };

  const requestConfirmation = (form) => {
    const modal = document.getElementById("ops-confirm-modal");
    if (!modal) {
      showToast("확인 화면을 불러오지 못했습니다.", "error");
      return;
    }
    pendingConfirmationForm = form;
    modal.querySelector("[data-confirm-title]").textContent =
      form.dataset.confirmTitle || "작업 확인";
    modal.querySelector("[data-confirm-message]").textContent =
      form.dataset.confirm || "이 작업을 계속하시겠습니까?";
    modal.querySelector("[data-confirm-accept]").textContent =
      form.dataset.confirmAction || "확인";
    openModal("ops-confirm-modal");
    window.setTimeout(() => modal.querySelector("[data-confirm-accept]")?.focus(), 30);
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

  const asyncSubmit = async (form, { confirmed = false } = {}) => {
    const confirmation = form.dataset.confirm;
    if (confirmation && !confirmed) {
      requestConfirmation(form);
      return;
    }
    const keepModalOpen = form.hasAttribute("data-keep-modal");
    const refreshAllianceHistory = form.hasAttribute("data-alliance-history-cancel");
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
      if (!keepModalOpen) closeModal(form);
      showToast(payload.message || "처리했습니다.");
      await refreshLivePage();
      if (refreshAllianceHistory) await loadAllianceHistory({ reset: true });
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

  const setFeeRuleEditor = (form, isOpen, { reset = false } = {}) => {
    if (!form) return;
    const view = form.querySelector("[data-fee-rule-view]");
    const editor = form.querySelector("[data-fee-rule-editor]");
    const toggle = form.querySelector("[data-fee-rule-edit]");
    if (!view || !editor || !toggle) return;
    if (reset) form.reset();
    view.hidden = isOpen;
    editor.hidden = !isOpen;
    form.classList.toggle("is-editing", isOpen);
    toggle.setAttribute("aria-expanded", String(isOpen));
    editor.querySelectorAll("input, select, textarea").forEach((control) => {
      control.disabled = !isOpen;
    });
    if (isOpen) {
      window.setTimeout(() => {
        editor.querySelector("input:not([type=hidden]), select, textarea")?.focus();
      }, 20);
    }
  };

  const saleBuyerUsers = () => {
    const data = document.querySelector("#drop-buyer-users-data");
    if (!data) return [];
    try {
      return JSON.parse(data.textContent || "[]");
    } catch {
      return [];
    }
  };

  const setSaleBuyer = (form, user = null) => {
    const field = form.querySelector('[name="buyer_user_id"]')?.closest("[data-user-picker-field]");
    if (!field) return;
    const hiddenInput = field.querySelector("[data-user-id]");
    const name = field.querySelector("[data-user-picker-name]");
    const meta = field.querySelector("[data-user-picker-meta]");
    const openButton = field.querySelector("[data-user-picker-open]");
    const clearButton = field.querySelector("[data-user-picker-clear]");
    hiddenInput.value = user ? String(user.user_id) : "";
    name.textContent = user?.display_name || "구매자 미지정";
    meta.textContent = user
      ? user.username || String(user.discord_id || user.user_id)
      : "구매 혈맹원 중 선택";
    openButton.classList.toggle("has-selection", Boolean(user));
    if (clearButton) clearButton.disabled = !user;
  };

  const openSaleBuyerPicker = async (button) => {
    const form = button.closest("[data-sale-form]");
    if (!form || !window.dashboardUserPicker) return;
    const allianceSelect = form.querySelector("[data-buyer-alliance]");
    const allianceId = allianceSelect?.value || "";
    if (!allianceId) {
      showToast("구매 혈맹을 먼저 선택해 주세요.", "error");
      allianceSelect?.focus();
      return;
    }
    const hiddenInput = form.querySelector('[name="buyer_user_id"]');
    const result = await window.dashboardUserPicker.open({
      users: saleBuyerUsers(),
      idKey: "user_id",
      multiple: false,
      title: "구매자 선택",
      allianceId,
      allianceName: allianceSelect.selectedOptions[0]?.textContent?.trim() || "",
      selectedIds: hiddenInput?.value ? [hiddenInput.value] : [],
    });
    if (result?.length) setSaleBuyer(form, result[0]);
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
    const buyer = saleBuyerUsers().find(
      (user) => String(user.user_id) === String(drop.buyer_user_id || ""),
    );
    setSaleBuyer(form, buyer || null);
    updateSalePreview(form);
    openModal("drop-sale-modal");
  };

  const dropHistoryState = {
    guildId: "",
    period: "30",
    dateFrom: "",
    dateTo: "",
    query: "",
    page: 1,
    request: 0,
    loading: false,
    hasNext: false,
    searchTimer: 0,
  };

  const renderDropHistoryCard = (record) => {
    const card = document.createElement("article");
    card.className = "drop-history-card";

    const heading = document.createElement("header");
    const identity = document.createElement("div");
    const itemName = document.createElement("strong");
    itemName.textContent = record.item_name || "이름 없는 아이템";
    const attendance = document.createElement("small");
    attendance.textContent =
      `출석 #${record.attendance_id || "-"} · ${record.occurred_at_label || "-"}`;
    identity.append(itemName, attendance);
    const dropNumber = document.createElement("span");
    dropNumber.textContent = `DROP #${record.drop_id || "-"}`;
    heading.append(identity, dropNumber);

    const metrics = document.createElement("div");
    metrics.className = "drop-history-card-metrics";
    [
      ["판매 원화", record.cash_label || "0원"],
      ["판매 아데나", record.adena_label || "0"],
      ["아데나 시세", record.rate_label || "-"],
      ["참여 인원", `${Number(record.participant_count || 0).toLocaleString("ko-KR")}명`],
    ].forEach(([label, value]) => {
      const metric = document.createElement("div");
      const labelNode = document.createElement("span");
      labelNode.textContent = label;
      const valueNode = document.createElement("strong");
      valueNode.textContent = value;
      metric.append(labelNode, valueNode);
      metrics.append(metric);
    });

    const footer = document.createElement("footer");
    const buyer = document.createElement("div");
    const buyerLabel = document.createElement("span");
    buyerLabel.textContent = "구매";
    const buyerName = document.createElement("strong");
    buyerName.textContent = record.buyer_user_name
      ? `${record.buyer_alliance_name || "-"} · ${record.buyer_user_name}`
      : record.buyer_alliance_name || "-";
    buyer.append(buyerLabel, buyerName);
    const completedAt = document.createElement("time");
    completedAt.textContent = `판매 완료 ${record.completed_at_label || "-"}`;
    footer.append(buyer, completedAt);

    card.append(heading, metrics, footer);
    return card;
  };

  const loadDropHistory = async ({ reset = false } = {}) => {
    const modal = document.querySelector("[data-drop-sale-history]");
    if (!modal || (dropHistoryState.loading && !reset)) return;
    if (reset && dropHistoryState.loading) {
      dropHistoryState.request += 1;
      dropHistoryState.loading = false;
    }
    if (reset) {
      dropHistoryState.page = 1;
      dropHistoryState.hasNext = false;
    }
    const requestNumber = ++dropHistoryState.request;
    const list = modal.querySelector("[data-drop-history-list]");
    const loading = modal.querySelector("[data-drop-history-loading]");
    const empty = modal.querySelector("[data-drop-history-empty]");
    const more = modal.querySelector("[data-drop-history-more]");
    dropHistoryState.loading = true;
    loading.hidden = false;
    loading.textContent = "판매 기록을 불러오는 중입니다.";
    empty.hidden = true;
    more.hidden = true;
    if (reset) {
      list.hidden = true;
      list.replaceChildren();
    }

    const params = new URLSearchParams({
      guild_id: dropHistoryState.guildId,
      period: dropHistoryState.period,
      q: dropHistoryState.query,
      page: String(dropHistoryState.page),
    });
    appendHistoryDateParams(params, dropHistoryState);
    try {
      const response = await fetch(`/api/drop-sale-history?${params.toString()}`, {
        headers: { Accept: "application/json" },
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok || !payload.ok) {
        throw new Error(payload.detail || payload.message || "판매 기록을 불러오지 못했습니다.");
      }
      if (requestNumber !== dropHistoryState.request) return;
      modal.querySelector("[data-drop-history-count]").textContent =
        `${Number(payload.summary?.total_count || 0).toLocaleString("ko-KR")}건`;
      modal.querySelector("[data-drop-history-cash]").textContent =
        payload.summary?.total_cash_label || "0원";
      modal.querySelector("[data-drop-history-adena]").textContent =
        payload.summary?.total_adena_label || "0";

      const records = Array.isArray(payload.history) ? payload.history : [];
      const fragment = document.createDocumentFragment();
      records.forEach((record) => fragment.append(renderDropHistoryCard(record)));
      list.append(fragment);
      list.hidden = list.children.length === 0;
      empty.hidden = list.children.length > 0;
      dropHistoryState.hasNext = Boolean(payload.pagination?.has_next);
      more.hidden = !dropHistoryState.hasNext;
      loading.hidden = true;
    } catch (error) {
      if (requestNumber !== dropHistoryState.request) return;
      loading.hidden = false;
      loading.textContent = error.message || "판매 기록을 불러오지 못했습니다.";
    } finally {
      if (requestNumber === dropHistoryState.request) {
        dropHistoryState.loading = false;
      }
    }
  };

  const openDropHistory = (button) => {
    const modal = document.querySelector("[data-drop-sale-history]");
    if (!modal) return;
    window.clearTimeout(dropHistoryState.searchTimer);
    dropHistoryState.guildId = button.dataset.guildId || modal.dataset.guildId || "";
    dropHistoryState.period = "30";
    dropHistoryState.dateFrom = "";
    dropHistoryState.dateTo = "";
    dropHistoryState.query = "";
    modal.querySelector("[data-drop-history-search]").value = "";
    modal.querySelectorAll("[data-drop-history-period]").forEach((periodButton) => {
      periodButton.classList.toggle("is-active", periodButton.dataset.dropHistoryPeriod === "30");
    });
    openModal("drop-sale-history-modal");
    loadDropHistory({ reset: true });
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

  const feeHistoryState = {
    feeRuleId: "",
    guildId: "",
    period: "30",
    dateFrom: "",
    dateTo: "",
    page: 1,
    request: 0,
    loading: false,
    hasNext: false,
  };

  const renderFeeHistoryRow = (record) => {
    const row = document.createElement("article");
    row.className = "fee-history-row";

    const item = document.createElement("div");
    item.className = "fee-history-item";
    const itemName = document.createElement("strong");
    itemName.textContent = record.item_name || "이름 없는 아이템";
    const context = document.createElement("small");
    const alliance = record.alliance_name ? ` · ${record.alliance_name}` : "";
    context.textContent = `출석 #${record.attendance_id || "-"}${alliance}`;
    item.append(itemName, context);

    const amount = document.createElement("div");
    amount.className = "fee-history-amount";
    const amountValue = document.createElement("strong");
    amountValue.textContent = Number(record.amount_adena || 0).toLocaleString("ko-KR");
    const amountUnit = document.createElement("small");
    amountUnit.textContent = "아데나";
    amount.append(amountValue, amountUnit);

    const occurred = document.createElement("div");
    occurred.className = "fee-history-date";
    const occurredLabel = document.createElement("strong");
    occurredLabel.textContent = record.occurred_at_label || "-";
    const completedLabel = document.createElement("small");
    completedLabel.textContent = record.completed_at_label
      ? `완료 ${record.completed_at_label}`
      : "정산 대기";
    occurred.append(occurredLabel, completedLabel);

    const status = document.createElement("span");
    status.className = `ops-status ops-status-${record.status_tone || "pending"}`;
    status.textContent = record.status_label || "미완료";

    row.append(item, amount, occurred, status);
    return row;
  };

  const loadFeeHistory = async ({ reset = false } = {}) => {
    const modal = document.querySelector("[data-fee-history-modal]");
    if (!modal || (feeHistoryState.loading && !reset)) return;
    if (reset && feeHistoryState.loading) {
      feeHistoryState.request += 1;
      feeHistoryState.loading = false;
    }
    if (reset) {
      feeHistoryState.page = 1;
      feeHistoryState.hasNext = false;
    }
    const requestNumber = ++feeHistoryState.request;
    const list = modal.querySelector("[data-fee-history-list]");
    const loading = modal.querySelector("[data-fee-history-loading]");
    const empty = modal.querySelector("[data-fee-history-empty]");
    const more = modal.querySelector("[data-fee-history-more]");
    feeHistoryState.loading = true;
    loading.hidden = false;
    loading.textContent = "기록을 불러오는 중입니다.";
    empty.hidden = true;
    more.hidden = true;
    if (reset) {
      list.hidden = true;
      list.replaceChildren();
    }

    const params = new URLSearchParams({
      guild_id: feeHistoryState.guildId,
      period: feeHistoryState.period,
      page: String(feeHistoryState.page),
    });
    appendHistoryDateParams(params, feeHistoryState);
    try {
      const response = await fetch(
        `/api/fee-rules/${encodeURIComponent(feeHistoryState.feeRuleId)}/history?${params.toString()}`,
        { headers: { Accept: "application/json" } },
      );
      const payload = await response.json().catch(() => ({}));
      if (!response.ok || !payload.ok) {
        throw new Error(payload.detail || payload.message || "수수료 기록을 불러오지 못했습니다.");
      }
      if (requestNumber !== feeHistoryState.request) return;
      modal.querySelector("[data-fee-history-name]").textContent =
        payload.fee_rule?.rule_name || "-";
      modal.querySelector("[data-fee-history-rate]").textContent =
        `현재 ${payload.fee_rule?.rate_label || "0%"}`;
      modal.querySelector("[data-fee-history-pending-amount]").textContent =
        payload.summary?.pending_amount_label || "0";
      modal.querySelector("[data-fee-history-pending-count]").textContent =
        `${Number(payload.summary?.pending_count || 0).toLocaleString("ko-KR")}건`;
      modal.querySelector("[data-fee-history-complete-amount]").textContent =
        payload.summary?.complete_amount_label || "0";
      modal.querySelector("[data-fee-history-complete-count]").textContent =
        `${Number(payload.summary?.complete_count || 0).toLocaleString("ko-KR")}건`;

      const records = Array.isArray(payload.history) ? payload.history : [];
      const fragment = document.createDocumentFragment();
      records.forEach((record) => fragment.append(renderFeeHistoryRow(record)));
      list.append(fragment);
      list.hidden = list.children.length === 0;
      empty.hidden = list.children.length > 0;
      feeHistoryState.hasNext = Boolean(payload.pagination?.has_next);
      more.hidden = !feeHistoryState.hasNext;
      loading.hidden = true;
    } catch (error) {
      if (requestNumber !== feeHistoryState.request) return;
      loading.hidden = false;
      loading.textContent = error.message || "수수료 기록을 불러오지 못했습니다.";
    } finally {
      if (requestNumber === feeHistoryState.request) {
        feeHistoryState.loading = false;
      }
    }
  };

  const openFeeHistory = (button) => {
    const modal = document.querySelector("[data-fee-history-modal]");
    if (!modal) return;
    feeHistoryState.feeRuleId = button.dataset.feeRuleId || "";
    feeHistoryState.guildId = button.dataset.guildId || "";
    feeHistoryState.period = "30";
    feeHistoryState.dateFrom = "";
    feeHistoryState.dateTo = "";
    resetHistoryPeriodButtons("[data-fee-history-period]", "feeHistoryPeriod");
    modal.querySelector("[data-fee-history-name]").textContent =
      button.dataset.feeRuleName || "-";
    openModal("fee-history-modal");
    loadFeeHistory({ reset: true });
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
    userId: "",
    userName: "",
    page: 1,
    status: "all",
    period: "30",
    dateFrom: "",
    dateTo: "",
    request: 0,
    loading: false,
    hasNext: false,
  };

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
    attendance.textContent = `${record.context_label || `출석 #${record.attendance_id || "-"}`} · ${record.occurred_at_label || "-"}`;
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
      user_id: clanHistoryState.userId,
      period: clanHistoryState.period,
      history_status: clanHistoryState.status,
      page: String(clanHistoryState.page),
    });
    appendHistoryDateParams(params, clanHistoryState);

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
    clanHistoryState.userId = button.dataset.userId || "";
    clanHistoryState.userName = button.dataset.userName || "알 수 없는 유저";
    clanHistoryState.status = "all";
    clanHistoryState.period = "30";
    clanHistoryState.dateFrom = "";
    clanHistoryState.dateTo = "";
    modal.querySelector("[data-history-user-name]").textContent = clanHistoryState.userName;
    modal.querySelectorAll("[data-history-status]").forEach((filter) => {
      filter.classList.toggle("is-active", filter.dataset.historyStatus === "all");
    });
    resetHistoryPeriodButtons("[data-clan-history-period]", "clanHistoryPeriod");
    openModal("clan-settlement-history-modal");
    loadClanHistory({ reset: true });
  };

  const clanItemHistoryState = {
    guildId: "",
    allianceId: "",
    period: "30",
    dateFrom: "",
    dateTo: "",
    query: "",
    page: 1,
    request: 0,
    loading: false,
    hasNext: false,
    searchTimer: 0,
  };

  const renderClanItemHistoryCard = (record) => {
    const card = document.createElement("article");
    card.className = "clan-item-history-card";

    const heading = document.createElement("header");
    const identity = document.createElement("div");
    const itemName = document.createElement("strong");
    itemName.textContent = record.item_name || "이름 없는 아이템";
    const context = document.createElement("small");
    context.textContent =
      `출석 #${record.attendance_id || "-"} · ${record.occurred_at_label || "-"}`;
    identity.append(itemName, context);
    const completed = document.createElement("time");
    completed.textContent = `완료 ${record.completed_at_label || "-"}`;
    heading.append(identity, completed);

    const metrics = document.createElement("div");
    metrics.className = "clan-item-history-metrics";
    [
      ["총 분배금", record.distribution_amount_label || "0", "is-primary"],
      ["지급 완료", record.paid_amount_label || "0", ""],
      ["총 혈비", record.clan_fund_amount_label || "0", "is-fund"],
      ["기타 수수료", record.custom_fee_amount_label || "0", ""],
    ].forEach(([label, value, tone]) => {
      const metric = document.createElement("div");
      if (tone) metric.className = tone;
      const labelNode = document.createElement("span");
      labelNode.textContent = label;
      const valueNode = document.createElement("strong");
      valueNode.textContent = value;
      const unit = document.createElement("small");
      unit.textContent = "아데나";
      metric.append(labelNode, valueNode, unit);
      metrics.append(metric);
    });

    const footer = document.createElement("footer");
    const paid = document.createElement("span");
    paid.textContent =
      `지급 ${Number(record.paid_member_count || 0).toLocaleString("ko-KR")}명`;
    const forfeited = document.createElement("span");
    forfeited.textContent =
      `귀속 ${Number(record.forfeited_member_count || 0).toLocaleString("ko-KR")}명 · ${record.forfeited_amount_label || "0"} 아데나`;
    footer.append(paid, forfeited);

    card.append(heading, metrics, footer);
    return card;
  };

  const loadClanItemHistory = async ({ reset = false } = {}) => {
    const modal = document.querySelector("[data-clan-item-history]");
    if (!modal || (clanItemHistoryState.loading && !reset)) return;
    if (reset && clanItemHistoryState.loading) {
      clanItemHistoryState.request += 1;
      clanItemHistoryState.loading = false;
    }
    if (reset) {
      clanItemHistoryState.page = 1;
      clanItemHistoryState.hasNext = false;
    }
    const requestNumber = ++clanItemHistoryState.request;
    const list = modal.querySelector("[data-clan-item-list]");
    const loading = modal.querySelector("[data-clan-item-loading]");
    const empty = modal.querySelector("[data-clan-item-empty]");
    const more = modal.querySelector("[data-clan-item-more]");
    clanItemHistoryState.loading = true;
    loading.hidden = false;
    loading.textContent = "완료 기록을 불러오는 중입니다.";
    empty.hidden = true;
    more.hidden = true;
    if (reset) {
      list.hidden = true;
      list.replaceChildren();
    }

    const params = new URLSearchParams({
      guild_id: clanItemHistoryState.guildId,
      alliance_id: clanItemHistoryState.allianceId,
      period: clanItemHistoryState.period,
      q: clanItemHistoryState.query,
      page: String(clanItemHistoryState.page),
    });
    appendHistoryDateParams(params, clanItemHistoryState);
    try {
      const response = await fetch(`/api/clan-completed-item-history?${params.toString()}`, {
        headers: { Accept: "application/json" },
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok || !payload.ok) {
        throw new Error(payload.detail || payload.message || "완료 기록을 불러오지 못했습니다.");
      }
      if (requestNumber !== clanItemHistoryState.request) return;
      modal.querySelector("[data-clan-item-count]").textContent =
        `${Number(payload.summary?.total_count || 0).toLocaleString("ko-KR")}건`;
      modal.querySelector("[data-clan-item-distribution]").textContent =
        payload.summary?.distribution_amount_label || "0";
      modal.querySelector("[data-clan-item-fund]").textContent =
        payload.summary?.clan_fund_amount_label || "0";
      modal.querySelector("[data-clan-item-fee]").textContent =
        payload.summary?.custom_fee_amount_label || "0";

      const records = Array.isArray(payload.history) ? payload.history : [];
      const fragment = document.createDocumentFragment();
      records.forEach((record) => fragment.append(renderClanItemHistoryCard(record)));
      list.append(fragment);
      list.hidden = list.children.length === 0;
      empty.hidden = list.children.length > 0;
      clanItemHistoryState.hasNext = Boolean(payload.pagination?.has_next);
      more.hidden = !clanItemHistoryState.hasNext;
      loading.hidden = true;
    } catch (error) {
      if (requestNumber !== clanItemHistoryState.request) return;
      loading.hidden = false;
      loading.textContent = error.message || "완료 기록을 불러오지 못했습니다.";
    } finally {
      if (requestNumber === clanItemHistoryState.request) {
        clanItemHistoryState.loading = false;
      }
    }
  };

  const openClanItemHistory = (button) => {
    const modal = document.querySelector("[data-clan-item-history]");
    if (!modal) return;
    window.clearTimeout(clanItemHistoryState.searchTimer);
    clanItemHistoryState.guildId = button.dataset.guildId || modal.dataset.guildId || "";
    clanItemHistoryState.allianceId =
      button.dataset.allianceId || modal.dataset.allianceId || "";
    clanItemHistoryState.period = "30";
    clanItemHistoryState.dateFrom = "";
    clanItemHistoryState.dateTo = "";
    clanItemHistoryState.query = "";
    modal.querySelector("[data-clan-item-search]").value = "";
    modal.querySelectorAll("[data-clan-item-period]").forEach((periodButton) => {
      periodButton.classList.toggle("is-active", periodButton.dataset.clanItemPeriod === "30");
    });
    openModal("clan-item-history-modal");
    loadClanItemHistory({ reset: true });
  };

  const allianceHistoryState = {
    guildId: "",
    allianceId: "",
    allianceName: "",
    period: "30",
    dateFrom: "",
    dateTo: "",
    page: 1,
    request: 0,
    loading: false,
    hasNext: false,
  };

  const renderAllianceHistoryRow = (record) => {
    const row = document.createElement("article");
    row.className = "alliance-history-row";

    const recordCopy = document.createElement("div");
    recordCopy.className = "alliance-history-record";
    const itemName = document.createElement("strong");
    itemName.textContent = record.item_name || "이름 없는 아이템";
    const attendance = document.createElement("span");
    attendance.textContent = `${record.context_label || `출석 #${record.attendance_id || "-"}`} · ${record.occurred_at_label || "-"}`;
    const completedAt = document.createElement("small");
    completedAt.textContent = `완료 ${record.completed_at_label || "-"}`;
    recordCopy.append(itemName, attendance, completedAt);

    const amount = document.createElement("div");
    amount.className = "alliance-history-amount";
    const amountValue = document.createElement("strong");
    amountValue.textContent = Number(record.amount_adena || 0).toLocaleString("ko-KR");
    const amountUnit = document.createElement("small");
    amountUnit.textContent = "분배 아데나";
    amount.append(amountValue, amountUnit);

    const progress = document.createElement("div");
    progress.className = `alliance-history-progress${record.can_cancel ? " is-cancellable" : ""}`;
    const progressLabel = document.createElement("strong");
    progressLabel.textContent = record.progress_label || "혈맹 분배 상태 확인";
    progress.append(progressLabel);

    row.append(recordCopy, amount, progress);
    if (record.can_cancel) {
      const form = document.createElement("form");
      form.method = "post";
      form.action = `/api/payouts/${record.payout_object_id}/status`;
      form.dataset.asyncForm = "";
      form.dataset.keepModal = "";
      form.dataset.allianceHistoryCancel = "";
      form.dataset.confirm = "이 혈맹 분배 완료를 취소하시겠습니까?";
      const status = document.createElement("input");
      status.type = "hidden";
      status.name = "status_code";
      status.value = "0";
      const button = document.createElement("button");
      button.className = "secondary-button";
      button.type = "submit";
      button.textContent = "완료 취소";
      form.append(status, button);
      row.append(form);
    }
    return row;
  };

  const loadAllianceHistory = async ({ reset = false } = {}) => {
    const modal = document.querySelector("[data-alliance-history-modal]");
    if (!modal || (allianceHistoryState.loading && !reset)) return;
    if (reset && allianceHistoryState.loading) {
      allianceHistoryState.request += 1;
      allianceHistoryState.loading = false;
    }
    if (reset) {
      allianceHistoryState.page = 1;
      allianceHistoryState.hasNext = false;
    }
    const requestNumber = ++allianceHistoryState.request;
    const list = modal.querySelector("[data-alliance-history-list]");
    const loading = modal.querySelector("[data-alliance-history-loading]");
    const empty = modal.querySelector("[data-alliance-history-empty]");
    const more = modal.querySelector("[data-alliance-history-more]");
    allianceHistoryState.loading = true;
    loading.hidden = false;
    loading.textContent = "완료 기록을 불러오는 중입니다.";
    empty.hidden = true;
    more.hidden = true;
    if (reset) {
      list.hidden = true;
      list.replaceChildren();
    }

    const params = new URLSearchParams({
      guild_id: allianceHistoryState.guildId,
      alliance_id: allianceHistoryState.allianceId,
      period: allianceHistoryState.period,
      page: String(allianceHistoryState.page),
    });
    appendHistoryDateParams(params, allianceHistoryState);
    try {
      const response = await fetch(`/api/alliance-settlement-history?${params.toString()}`, {
        headers: { Accept: "application/json" },
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok || !payload.ok) {
        throw new Error(payload.detail || payload.message || "완료 기록을 불러오지 못했습니다.");
      }
      if (requestNumber !== allianceHistoryState.request) return;
      modal.querySelector("[data-alliance-history-name]").textContent =
        payload.alliance_name || allianceHistoryState.allianceName || "-";
      modal.querySelector("[data-alliance-history-count]").textContent =
        `${Number(payload.summary?.complete_count || 0).toLocaleString("ko-KR")}건`;
      modal.querySelector("[data-alliance-history-amount]").textContent =
        Number(String(payload.summary?.total_amount_label || "0").replaceAll(",", "")).toLocaleString("ko-KR");

      const records = Array.isArray(payload.history) ? payload.history : [];
      const fragment = document.createDocumentFragment();
      records.forEach((record) => fragment.append(renderAllianceHistoryRow(record)));
      list.append(fragment);
      list.hidden = list.children.length === 0;
      empty.hidden = list.children.length > 0;
      allianceHistoryState.hasNext = Boolean(payload.pagination?.has_next);
      more.hidden = !allianceHistoryState.hasNext;
      loading.hidden = true;
    } catch (error) {
      if (requestNumber !== allianceHistoryState.request) return;
      loading.hidden = false;
      loading.textContent = error.message || "완료 기록을 불러오지 못했습니다.";
    } finally {
      if (requestNumber === allianceHistoryState.request) allianceHistoryState.loading = false;
    }
  };

  const openAllianceHistory = (button) => {
    const modal = document.querySelector("[data-alliance-history-modal]");
    if (!modal) return;
    allianceHistoryState.guildId = button.dataset.guildId || "";
    allianceHistoryState.allianceId = button.dataset.allianceId || "";
    allianceHistoryState.allianceName = button.dataset.allianceName || "";
    allianceHistoryState.period = "30";
    allianceHistoryState.dateFrom = "";
    allianceHistoryState.dateTo = "";
    resetHistoryPeriodButtons(
      "[data-alliance-history-period]",
      "allianceHistoryPeriod",
    );
    modal.querySelector("[data-alliance-history-name]").textContent =
      allianceHistoryState.allianceName || "-";
    openModal("alliance-settlement-history-modal");
    loadAllianceHistory({ reset: true });
  };

  document.addEventListener("submit", (event) => {
    const historyDateForm = event.target.closest("[data-history-date-form]");
    if (historyDateForm) {
      event.preventDefault();
      event.stopPropagation();
      const dateFrom = historyDateForm.elements.date_from.value;
      const dateTo = historyDateForm.elements.date_to.value;
      const error = historyDateForm.querySelector("[data-history-date-error]");
      if (!dateFrom || !dateTo || dateFrom > dateTo) {
        error.textContent = !dateFrom || !dateTo
          ? "시작일과 종료일을 모두 선택해 주세요."
          : "종료일은 시작일보다 빠를 수 없습니다.";
        error.hidden = false;
        return;
      }
      const apply = historyDateApply;
      historyDateApply = null;
      closeModal(historyDateForm);
      if (apply) apply({ dateFrom, dateTo });
      return;
    }
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
    const confirmCancel = event.target.closest("[data-confirm-cancel]");
    if (confirmCancel) {
      event.preventDefault();
      closeConfirmation();
      return;
    }
    const confirmAccept = event.target.closest("[data-confirm-accept]");
    if (confirmAccept) {
      event.preventDefault();
      const form = pendingConfirmationForm;
      pendingConfirmationForm = null;
      closeModal(document.getElementById("ops-confirm-modal"));
      if (form) asyncSubmit(form, { confirmed: true });
      return;
    }
    if (event.target.closest("summary form, summary button")) event.stopPropagation();
    const modalOpen = event.target.closest("[data-modal-open]");
    if (modalOpen) openModal(modalOpen.dataset.modalOpen);
    const modalClose = event.target.closest("[data-modal-close]");
    if (modalClose) {
      modalClose.closest(".ops-modal")?.querySelectorAll("[data-fee-rule-form]").forEach((form) => {
        setFeeRuleEditor(form, false, { reset: true });
      });
      closeModal(modalClose);
    }
    const dropEdit = event.target.closest("[data-drop-edit]");
    if (dropEdit) populateDropEdit(dropEdit);
    const saleOpen = event.target.closest("[data-sale-open]");
    if (saleOpen) populateSale(saleOpen);
    const dropHistoryOpen = event.target.closest("[data-drop-sale-history-open]");
    if (dropHistoryOpen) openDropHistory(dropHistoryOpen);
    const dropHistoryPeriod = event.target.closest("[data-drop-history-period]");
    if (dropHistoryPeriod) {
      window.clearTimeout(dropHistoryState.searchTimer);
      selectHistoryPeriod({
        button: dropHistoryPeriod,
        state: dropHistoryState,
        datasetKey: "dropHistoryPeriod",
        buttonSelector: "[data-drop-history-period]",
        load: loadDropHistory,
      });
    }
    const dropHistoryMore = event.target.closest("[data-drop-history-more]");
    if (dropHistoryMore && dropHistoryState.hasNext) {
      dropHistoryState.page += 1;
      loadDropHistory();
    }
    const saleBuyerOpen = event.target.closest("[data-sale-form] [data-user-picker-open]");
    if (saleBuyerOpen) openSaleBuyerPicker(saleBuyerOpen);
    const saleBuyerClear = event.target.closest("[data-sale-form] [data-user-picker-clear]");
    if (saleBuyerClear) setSaleBuyer(saleBuyerClear.closest("[data-sale-form]"));
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
    const feeRuleEdit = event.target.closest("[data-fee-rule-edit]");
    if (feeRuleEdit) {
      const form = feeRuleEdit.closest("[data-fee-rule-form]");
      document.querySelectorAll("[data-fee-rule-form].is-editing").forEach((openForm) => {
        if (openForm !== form) setFeeRuleEditor(openForm, false, { reset: true });
      });
      setFeeRuleEditor(form, true);
    }
    const feeRuleCancel = event.target.closest("[data-fee-rule-cancel]");
    if (feeRuleCancel) {
      setFeeRuleEditor(feeRuleCancel.closest("[data-fee-rule-form]"), false, { reset: true });
    }
    const feeEdit = event.target.closest("[data-fee-edit]");
    if (feeEdit) populateFeeEdit(feeEdit);
    const feeHistoryOpen = event.target.closest("[data-fee-history-open]");
    if (feeHistoryOpen) openFeeHistory(feeHistoryOpen);
    const feeHistoryPeriod = event.target.closest("[data-fee-history-period]");
    if (feeHistoryPeriod) {
      selectHistoryPeriod({
        button: feeHistoryPeriod,
        state: feeHistoryState,
        datasetKey: "feeHistoryPeriod",
        buttonSelector: "[data-fee-history-period]",
        load: loadFeeHistory,
      });
    }
    const feeHistoryMore = event.target.closest("[data-fee-history-more]");
    if (feeHistoryMore && feeHistoryState.hasNext) {
      feeHistoryState.page += 1;
      loadFeeHistory();
    }
    const bidHistoryOpen = event.target.closest("[data-bid-history-open]");
    if (bidHistoryOpen) openBidHistory(bidHistoryOpen);
    const clanHistoryOpen = event.target.closest("[data-clan-history-open]");
    if (clanHistoryOpen) openClanHistory(clanHistoryOpen);
    const clanItemHistoryOpen = event.target.closest("[data-clan-item-history-open]");
    if (clanItemHistoryOpen) openClanItemHistory(clanItemHistoryOpen);
    const clanItemPeriod = event.target.closest("[data-clan-item-period]");
    if (clanItemPeriod) {
      window.clearTimeout(clanItemHistoryState.searchTimer);
      selectHistoryPeriod({
        button: clanItemPeriod,
        state: clanItemHistoryState,
        datasetKey: "clanItemPeriod",
        buttonSelector: "[data-clan-item-period]",
        load: loadClanItemHistory,
      });
    }
    const clanItemMore = event.target.closest("[data-clan-item-more]");
    if (clanItemMore && clanItemHistoryState.hasNext) {
      clanItemHistoryState.page += 1;
      loadClanItemHistory();
    }
    const allianceHistoryOpen = event.target.closest("[data-alliance-history-open]");
    if (allianceHistoryOpen) {
      event.preventDefault();
      openAllianceHistory(allianceHistoryOpen);
    }
    const allianceHistoryPeriod = event.target.closest("[data-alliance-history-period]");
    if (allianceHistoryPeriod) {
      selectHistoryPeriod({
        button: allianceHistoryPeriod,
        state: allianceHistoryState,
        datasetKey: "allianceHistoryPeriod",
        buttonSelector: "[data-alliance-history-period]",
        load: loadAllianceHistory,
      });
    }
    const clanHistoryStatus = event.target.closest("[data-history-status]");
    if (clanHistoryStatus) {
      clanHistoryState.status = clanHistoryStatus.dataset.historyStatus || "all";
      clanHistoryStatus.parentElement.querySelectorAll("[data-history-status]").forEach((filter) => {
        filter.classList.toggle("is-active", filter === clanHistoryStatus);
      });
      loadClanHistory({ reset: true });
    }
    const clanHistoryPeriod = event.target.closest("[data-clan-history-period]");
    if (clanHistoryPeriod) {
      selectHistoryPeriod({
        button: clanHistoryPeriod,
        state: clanHistoryState,
        datasetKey: "clanHistoryPeriod",
        buttonSelector: "[data-clan-history-period]",
        load: loadClanHistory,
      });
    }
    const clanHistoryMore = event.target.closest("[data-history-more]");
    if (clanHistoryMore && clanHistoryState.hasNext) {
      clanHistoryState.page += 1;
      loadClanHistory();
    }
    const allianceHistoryMore = event.target.closest("[data-alliance-history-more]");
    if (allianceHistoryMore && allianceHistoryState.hasNext) {
      allianceHistoryState.page += 1;
      loadAllianceHistory();
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
    if (buyerAlliance) setSaleBuyer(buyerAlliance.form);
  });

  document.addEventListener("input", (event) => {
    const clientSearchInput = event.target.closest("[data-client-search-input]");
    if (clientSearchInput) applyClientSearch(clientSearchInput);
    const dropHistorySearch = event.target.closest("[data-drop-history-search]");
    if (dropHistorySearch) {
      window.clearTimeout(dropHistoryState.searchTimer);
      dropHistoryState.searchTimer = window.setTimeout(() => {
        dropHistoryState.query = dropHistorySearch.value.trim();
        loadDropHistory({ reset: true });
      }, 280);
    }
    const clanItemSearch = event.target.closest("[data-clan-item-search]");
    if (clanItemSearch) {
      window.clearTimeout(clanItemHistoryState.searchTimer);
      clanItemHistoryState.searchTimer = window.setTimeout(() => {
        clanItemHistoryState.query = clanItemSearch.value.trim();
        loadClanItemHistory({ reset: true });
      }, 280);
    }
    const saleInput = event.target.closest("[data-sale-rate]");
    if (saleInput) updateSalePreview(saleInput.form);
  });

  document.addEventListener("keydown", (event) => {
    if (event.key !== "Escape") return;
    if (document.getElementById("ops-confirm-modal")?.classList.contains("is-open")) {
      closeConfirmation();
      return;
    }
    const openModals = [...document.querySelectorAll(".ops-modal.is-open")];
    closeModal(openModals.at(-1));
  });

  const initializeOperationsPage = () => {
    document.querySelectorAll("[data-client-search-input]").forEach(applyClientSearch);
  };

  document.addEventListener("dashboard:page-loaded", initializeOperationsPage);
  initializeOperationsPage();
})();
