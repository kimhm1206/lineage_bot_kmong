(() => {
  const state = {
    attendance: null,
    memberOptions: [],
    memberOptionsGuild: "",
  };

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
    window.requestAnimationFrame(() => toast.classList.add("is-visible"));
    window.setTimeout(() => {
      toast.classList.remove("is-visible");
      window.setTimeout(() => toast.remove(), 180);
    }, 3200);
  };

  const normalized = (value) =>
    String(value || "").normalize("NFKC").toLocaleLowerCase("ko-KR").replace(/\s+/g, " ").trim();

  const editor = () => document.querySelector("[data-attendance-editor]");

  const members = (attendance = state.attendance) =>
    (attendance?.alliances || []).flatMap((alliance) =>
      (alliance.members || []).map((member) => ({
        ...member,
        alliance_id: alliance.alliance_id,
        alliance_name: alliance.alliance_name,
      })),
    );

  const createMemberRow = (member) => {
    const row = document.createElement("article");
    row.className = "attendance-editor-row";
    row.dataset.searchText = normalized(`${member.discord_nickname} ${member.alliance_name}`);

    const identity = document.createElement("div");
    const name = document.createElement("strong");
    name.textContent = member.discord_nickname;
    const alliance = document.createElement("span");
    alliance.textContent = member.alliance_name || "미분류";
    identity.append(name, alliance);

    const remove = document.createElement("button");
    remove.type = "button";
    remove.className = "danger-button attendance-member-delete";
    remove.textContent = "삭제";
    remove.dataset.attendanceMemberDelete = String(member.user_id);
    remove.dataset.memberName = member.discord_nickname;
    row.append(identity, remove);
    return row;
  };

  const renderEditorMembers = () => {
    const root = editor();
    if (!root || !state.attendance) return;
    const list = root.querySelector("[data-attendance-member-list]");
    const empty = root.querySelector("[data-attendance-member-empty]");
    const query = normalized(root.querySelector("[data-attendance-member-search]")?.value);
    const visible = members().filter((member) =>
      !query || normalized(`${member.discord_nickname} ${member.alliance_name}`).includes(query),
    );
    list.replaceChildren(...visible.map(createMemberRow));
    empty.hidden = visible.length > 0;
  };

  const renderEditor = () => {
    const root = editor();
    if (!root || !state.attendance) return;
    root.querySelector("[data-attendance-editor-id]").textContent = `#${state.attendance.attendance_id}`;
    root.querySelector("[data-attendance-editor-time]").textContent = state.attendance.started_at_label;
    root.querySelector("[data-attendance-editor-count]").textContent = state.attendance.participant_label;
    const search = root.querySelector("[data-attendance-member-search]");
    if (search) search.value = "";
    renderEditorMembers();
  };

  const attendanceFromCard = (button) => {
    const card = button.closest("[data-attendance-card]");
    const alliances = [...card.querySelectorAll("[data-attendance-alliance-group]")].map((group) => {
      const allianceId = Number(group.dataset.allianceId);
      const alliance = {
        alliance_id: allianceId > 0 ? allianceId : null,
        alliance_name: group.dataset.allianceName || "미분류",
        members: [...group.querySelectorAll("[data-attendance-member]")].map((member) => ({
          user_id: Number(member.dataset.userId),
          discord_nickname: member.textContent.trim(),
        })),
      };
      alliance.count = alliance.members.length;
      return alliance;
    });
    return {
      attendance_id: Number(button.dataset.attendanceId),
      started_at_label: button.dataset.attendanceTime || "-",
      started_by: button.dataset.attendanceStarter || "-",
      participant_count: Number(button.dataset.attendanceCount) || 0,
      participant_label: `${Number(button.dataset.attendanceCount) || 0}명`,
      alliances,
    };
  };

  const openEditor = (button) => {
    state.attendance = attendanceFromCard(button);
    renderEditor();
    const modal = document.getElementById("attendance-edit-modal");
    if (modal && !modal.classList.contains("is-open")) {
      modal.classList.add("is-open");
      modal.setAttribute("aria-hidden", "false");
      document.body.classList.add("has-open-modal");
    }
  };

  const createAllianceChip = (alliance) => {
    const chip = document.createElement("span");
    chip.append(document.createTextNode(`${alliance.alliance_name} `));
    const count = document.createElement("b");
    count.textContent = `${alliance.count}명`;
    chip.append(count);
    return chip;
  };

  const createAllianceGroup = (alliance) => {
    const group = document.createElement("article");
    group.className = "attendance-alliance-group";
    group.dataset.attendanceAllianceGroup = "";
    group.dataset.allianceId = alliance.alliance_id || "";
    group.dataset.allianceName = alliance.alliance_name;
    const header = document.createElement("header");
    const title = document.createElement("h3");
    title.textContent = alliance.alliance_name;
    const count = document.createElement("span");
    count.textContent = `${alliance.count}명`;
    header.append(title, count);
    const list = document.createElement("div");
    list.className = "attendance-member-list";
    (alliance.members || []).forEach((member) => {
      const name = document.createElement("span");
      name.textContent = member.discord_nickname;
      name.dataset.attendanceMember = "";
      name.dataset.userId = String(member.user_id);
      list.append(name);
    });
    group.append(header, list);
    return group;
  };

  const updateAttendanceCard = (attendance) => {
    const card = document.getElementById(`attendance-${attendance.attendance_id}`);
    if (!card) return;
    const label = card.querySelector("[data-attendance-participant-label]");
    if (label) label.textContent = attendance.participant_label;

    const chips = card.querySelector("[data-attendance-alliance-chips]");
    if (chips) {
      if (attendance.alliances.length) {
        chips.replaceChildren(...attendance.alliances.map(createAllianceChip));
      } else {
        const empty = document.createElement("span");
        empty.className = "is-empty";
        empty.textContent = "출석 없음";
        chips.replaceChildren(empty);
      }
    }

    const detail = card.querySelector("[data-attendance-session-detail]");
    if (detail) {
      if (attendance.alliances.length) {
        detail.replaceChildren(...attendance.alliances.map(createAllianceGroup));
      } else {
        const empty = document.createElement("div");
        empty.className = "attendance-empty";
        empty.textContent = "이 회차에는 출석자가 없습니다.";
        detail.replaceChildren(empty);
      }
    }
    const editButton = card.querySelector("[data-attendance-edit]");
    if (editButton) {
      editButton.dataset.attendanceTime = attendance.started_at_label;
      editButton.dataset.attendanceStarter = attendance.started_by;
      editButton.dataset.attendanceCount = String(attendance.participant_count);
    }
  };

  const post = async (url, formData) => {
    const response = await fetch(url, {
      method: "POST",
      body: formData,
      headers: { Accept: "application/json" },
    });
    let payload = {};
    try {
      payload = await response.json();
    } catch {
      throw new Error("서버 응답을 확인할 수 없습니다.");
    }
    if (!response.ok || !payload.ok) {
      throw new Error(payload.message || payload.detail || "요청을 처리하지 못했습니다.");
    }
    return payload;
  };

  const applyMutation = (payload) => {
    state.attendance = payload.attendance;
    updateAttendanceCard(payload.attendance);
    renderEditor();
    showToast(payload.message);
  };

  const addMembers = async (button) => {
    if (!state.attendance || !window.dashboardUserPicker) {
      showToast("유저 선택기를 준비하지 못했습니다.", "error");
      return;
    }
    const root = editor();
    const guildId = root?.dataset.guildId;
    const original = button.innerHTML;
    if (state.memberOptionsGuild !== guildId) {
      button.disabled = true;
      button.textContent = "유저 불러오는 중";
      try {
        const response = await fetch(`/api/attendance/member-options?guild_id=${encodeURIComponent(guildId)}`, {
          headers: { Accept: "application/json" },
        });
        const payload = await response.json();
        if (!response.ok || !payload.ok) {
          throw new Error(payload.message || payload.detail || "서버 유저를 불러오지 못했습니다.");
        }
        state.memberOptions = payload.users || [];
        state.memberOptionsGuild = guildId;
      } catch (error) {
        showToast(error.message, "error");
        return;
      } finally {
        button.disabled = false;
        button.innerHTML = original;
      }
    }
    const selected = await window.dashboardUserPicker.open({
      users: state.memberOptions,
      idKey: "user_id",
      multiple: true,
      title: `#${state.attendance.attendance_id} 누락 인원 추가`,
      excludedIds: members().map((member) => member.user_id),
    });
    if (!selected?.length) return;

    const formData = new FormData();
    formData.append("guild_id", guildId);
    selected.forEach((user) => formData.append("user_ids", user.user_id));
    button.disabled = true;
    button.textContent = "추가 중";
    try {
      const payload = await post(
        `/api/attendance/${state.attendance.attendance_id}/members`,
        formData,
      );
      applyMutation(payload);
    } catch (error) {
      showToast(error.message, "error");
    } finally {
      button.disabled = false;
      button.innerHTML = original;
    }
  };

  const deleteMember = async (button) => {
    if (!state.attendance) return;
    const memberName = button.dataset.memberName || "선택한 유저";
    if (!window.confirm(`${memberName} 유저를 이 출석 회차에서 삭제할까요?`)) return;
    const root = editor();
    const formData = new FormData();
    formData.append("guild_id", root?.dataset.guildId || "");
    button.disabled = true;
    button.textContent = "처리 중";
    try {
      const payload = await post(
        `/api/attendance/${state.attendance.attendance_id}/members/${button.dataset.attendanceMemberDelete}/delete`,
        formData,
      );
      applyMutation(payload);
    } catch (error) {
      showToast(error.message, "error");
      button.disabled = false;
      button.textContent = "삭제";
    }
  };

  document.addEventListener("click", (event) => {
    const editButton = event.target.closest("[data-attendance-edit]");
    if (editButton) {
      event.preventDefault();
      openEditor(editButton);
      return;
    }
    const addButton = event.target.closest("[data-attendance-member-add]");
    if (addButton) {
      addMembers(addButton);
      return;
    }
    const deleteButton = event.target.closest("[data-attendance-member-delete]");
    if (deleteButton) deleteMember(deleteButton);
  });

  document.addEventListener("input", (event) => {
    if (event.target.matches("[data-attendance-member-search]")) renderEditorMembers();
  });
})();
