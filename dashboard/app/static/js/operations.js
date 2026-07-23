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
      page: String(feeHistoryState.page),
    });
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
      modal.querySelector("[data-fee-history-name]").textContent = payload.fee_rule?.rule_name || "-";
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
      if (requestNumber === feeHistoryState.request) feeHistoryState.loading = false;
    }
  };

  const openFeeHistory = (button) => {
    const modal = document.querySelector("[data-fee-history-modal]");
    if (!modal) return;
    feeHistoryState.feeRuleId = button.dataset.feeRuleId || "";
    feeHistoryState.guildId = button.dataset.guildId || "";
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

  const allianceHistoryState = {
    guildId: "",
    allianceId: "",
    allianceName: "",
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
      page: String(allianceHistoryState.page),
    });
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
    modal.querySelector("[data-alliance-history-name]").textContent =
      allianceHistoryState.allianceName || "-";
    openModal("alliance-settlement-history-modal");
    loadAllianceHistory({ reset: true });
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
    const feeEdit = event.target.closest("[data-fee-edit]");
    if (feeEdit) populateFeeEdit(feeEdit);
    const feeHistoryOpen = event.target.closest("[data-fee-history-open]");
    if (feeHistoryOpen) openFeeHistory(feeHistoryOpen);
    const feeHistoryMore = event.target.closest("[data-fee-history-more]");
    if (feeHistoryMore && feeHistoryState.hasNext) {
      feeHistoryState.page += 1;
      loadFeeHistory();
    }
    const bidHistoryOpen = event.target.closest("[data-bid-history-open]");
    if (bidHistoryOpen) openBidHistory(bidHistoryOpen);
    const clanHistoryOpen = event.target.closest("[data-clan-history-open]");
    if (clanHistoryOpen) openClanHistory(clanHistoryOpen);
    const allianceHistoryOpen = event.target.closest("[data-alliance-history-open]");
    if (allianceHistoryOpen) {
      event.preventDefault();
      openAllianceHistory(allianceHistoryOpen);
    }
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
