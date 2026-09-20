// @vitest-environment happy-dom
// AclsPanel 组件测试：空过滤查询行内拦截（不过桥）/ 具体过滤查询渲染 /
// 创建弹窗空名校验（acls.createInvalid）/ 创建成功后按新过滤重载 /
// 删除确认弹窗（过滤 JSON 回显 + 提交 matched 数）/ canWrite/canDelete 门禁 /
// 行点击详情抽屉。
import { beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount } from "@vue/test-utils";
import { defineComponent, h, type PropType } from "vue";
import AclsPanel from "./AclsPanel.vue";
import { setKafkaConnectionId, type KafkaAcl } from "../lib/api";
import { t } from "../lib/i18n";

// -- DbxAgGrid 轻量 stub（镜像真实桥形状，见 GroupsPanel.spec 同款） -----------------
const DbxAgGridStub = defineComponent({
  name: "DbxAgGridStub",
  props: {
    rowData: { type: Array as PropType<unknown[]>, default: () => [] },
    tableKey: { type: String, default: "" },
    rowSelection: { type: [String, Boolean] as PropType<"single" | false>, default: "single" as const },
    emitRowClick: { type: Boolean, default: true },
  },
  emits: ["selection-changed", "row-click"],
  setup(props, { emit }) {
    return () =>
      h(
        "div",
        { class: "grid-stub", "data-key": props.tableKey },
        (props.rowData ?? []).map((row, index) =>
          h(
            "button",
            {
              type: "button",
              class: "grid-stub-row",
              onClick: () => {
                if (props.rowSelection) emit("selection-changed", row);
                if (props.emitRowClick) emit("row-click", row);
              },
            },
            `${props.tableKey}-row-${index}`,
          ),
        ),
      );
  },
});

const invokeMock = vi.fn();

/** method → 响应 路由表；未命中抛错（对应未注册方法）；`{ error }` 信封按真实桥
 *  形态以异常拒绝（JSON-RPC error → invoke rejection → api 层转译为 Error）。 */
function installBridge(routes: Record<string, unknown>) {
  invokeMock.mockReset();
  invokeMock.mockImplementation(async (method: string) => {
    if (method in routes) {
      const result = routes[method];
      const envelope = result as { error?: { message?: string } } | null;
      if (envelope && typeof envelope === "object" && envelope.error) {
        throw new Error(envelope.error.message ?? "request failed");
      }
      return result;
    }
    throw new Error(`unhandled method: ${method}`);
  });
  (window as unknown as { dbxPlugin: unknown }).dbxPlugin = { invoke: invokeMock };
}

const acl: KafkaAcl = {
  resourceType: "TOPIC",
  resourceName: "order-events",
  patternType: "LITERAL",
  principal: "User:billing",
  host: "*",
  operation: "READ",
  permission: "ALLOW",
};

function mountPanel(props: Record<string, unknown> = {}) {
  return mount(AclsPanel, {
    props: { canWrite: true, canDelete: true, ...props },
    global: { stubs: { DbxAgGrid: DbxAgGridStub, teleport: true } },
  });
}

/** 过滤区输入：0 = resourceName，1 = principal。 */
function filterInputs(wrapper: ReturnType<typeof mountPanel>) {
  return wrapper.findAll('.kafka-form input[type="text"]');
}

async function queryByResourceName(wrapper: ReturnType<typeof mountPanel>, name: string) {
  await filterInputs(wrapper)[0].setValue(name);
  await wrapper.find(".kafka-form .primary-button").trigger("click");
  await flushPromises();
}

beforeEach(() => {
  localStorage.clear();
  setKafkaConnectionId("conn-test");
});

describe("AclsPanel", () => {
  it("blocks a broad filter query inline without calling the bridge", async () => {
    installBridge({});
    const wrapper = mountPanel();
    await flushPromises();
    await wrapper.find(".kafka-form .primary-button").trigger("click");
    await flushPromises();
    expect(wrapper.find(".form-error").text()).toBe(t("acls.filterTooBroad"));
    expect(invokeMock).not.toHaveBeenCalled();
    // 过宽被拒只留拒绝提示，不再叠加「没有匹配」空态（空态引导只属于未查询态）。
    expect(wrapper.find(".acls-empty").exists()).toBe(false);
  });

  it("lists ACLs for a concrete filter and renders rows", async () => {
    installBridge({ "kafka/acls/list": { acls: [acl] } });
    const wrapper = mountPanel();
    await flushPromises();
    await queryByResourceName(wrapper, "order-events");
    expect(invokeMock.mock.calls.find(([method]) => method === "kafka/acls/list")?.[1]).toMatchObject({
      filter: { resourceType: "ANY", resourceName: "order-events" },
    });
    // 清屏用的空串 error 事件 + 行渲染
    expect(wrapper.emitted("error")?.at(-1)).toEqual([""]);
    expect(wrapper.find('.grid-stub[data-key="acls"]').exists()).toBe(true);
    expect(wrapper.findAll(".grid-stub-row")).toHaveLength(1);
  });

  it("rejects a create with empty resource name or principal", async () => {
    installBridge({});
    const wrapper = mountPanel();
    await flushPromises();
    await wrapper.find(".kafka-form .toolbar-button").trigger("click");
    const modal = () => wrapper.find(".modal-backdrop .modal");
    expect(modal().exists()).toBe(true);
    // 空表单直接保存 → 行内校验，不弹桥、弹窗不关
    await modal().find("footer .primary-button").trigger("click");
    await flushPromises();
    expect(wrapper.emitted("error")?.at(-1)).toEqual([t("acls.createInvalid")]);
    expect(modal().exists()).toBe(true);
    expect(invokeMock).not.toHaveBeenCalled();
  });

  it("creates an ACL and reloads the list with its filter", async () => {
    installBridge({ "kafka/acls/list": { acls: [] }, "kafka/acls/create": { success: true } });
    const wrapper = mountPanel();
    await flushPromises();
    await wrapper.find(".kafka-form .toolbar-button").trigger("click");
    // 创建弹窗输入：0 = resourceName，1 = principal，2 = host
    const inputs = () => wrapper.findAll(".modal-backdrop .settings-field input[type=\"text\"]");
    await inputs()[0].setValue("order-events");
    await inputs()[1].setValue("User:app");
    await wrapper.find(".modal-backdrop footer .primary-button").trigger("click");
    await flushPromises();
    expect(invokeMock.mock.calls.find(([method]) => method === "kafka/acls/create")?.[1]).toMatchObject({
      acl: { resourceType: "TOPIC", resourceName: "order-events", principal: "User:app", operation: "READ", permission: "ALLOW" },
    });
    expect(wrapper.emitted("notify")?.at(-1)).toEqual([t("acls.created")]);
    // 弹窗关闭 + 按创建主体自动重查（filter 回填 resourceName/principal）
    expect(wrapper.find(".modal-backdrop").exists()).toBe(false);
    const listCall = invokeMock.mock.calls.find(([method]) => method === "kafka/acls/list");
    expect(listCall?.[1]).toMatchObject({
      filter: { resourceType: "TOPIC", resourceName: "order-events", patternType: "LITERAL", principal: "User:app" },
    });
  });

  it("shows the filter JSON in the delete dialog and submits deletion", async () => {
    installBridge({ "kafka/acls/list": { acls: [acl] }, "kafka/acls/delete": { matched: 1 } });
    const wrapper = mountPanel();
    await flushPromises();
    await queryByResourceName(wrapper, "order-events");
    // 有行才可删
    const deleteButton = wrapper.find(".kafka-form .danger-button");
    expect(deleteButton.attributes("disabled")).toBeUndefined();
    await deleteButton.trigger("click");
    const modal = () => wrapper.find(".modal-backdrop .modal");
    expect(modal().find("h2").text()).toBe(t("acls.deleteTitle"));
    // 过滤条件 JSON 回显
    expect(modal().find(".mono-s").text()).toBe(
      JSON.stringify({ resourceType: "ANY", resourceName: "order-events" }),
    );
    await modal().find("footer .danger-button").trigger("click");
    await flushPromises();
    expect(invokeMock.mock.calls.find(([method]) => method === "kafka/acls/delete")?.[1]).toMatchObject({
      filter: { resourceType: "ANY", resourceName: "order-events" },
    });
    expect(wrapper.emitted("notify")?.at(-1)).toEqual([t("acls.deleted", { count: 1 })]);
    expect(wrapper.find(".modal-backdrop").exists()).toBe(false);
    // 删除后重载列表
    expect(invokeMock.mock.calls.filter(([method]) => method === "kafka/acls/list")).toHaveLength(2);
  });

  it("gates create and delete buttons by canWrite/canDelete and rows", async () => {
    installBridge({ "kafka/acls/list": { acls: [acl] } });
    const wrapper = mountPanel({ canWrite: false, canDelete: false });
    await flushPromises();
    const createButton = wrapper.find(".kafka-form .toolbar-button");
    const deleteButton = wrapper.find(".kafka-form .danger-button");
    expect(createButton.attributes("disabled")).toBeDefined();
    expect(createButton.attributes("title")).toBe(t("readOnly"));
    expect(deleteButton.attributes("disabled")).toBeDefined();
    expect(deleteButton.attributes("title")).toBe(t("noDelete"));
    wrapper.unmount();
    // 可删连接但无行：Delete 仍禁用
    installBridge({ "kafka/acls/list": { acls: [] } });
    const emptyWrapper = mountPanel();
    await flushPromises();
    expect(emptyWrapper.find(".kafka-form .danger-button").attributes("disabled")).toBeDefined();
  });

  it("opens the detail drawer from a row click", async () => {
    installBridge({ "kafka/acls/list": { acls: [acl] } });
    const wrapper = mountPanel();
    await flushPromises();
    await queryByResourceName(wrapper, "order-events");
    await wrapper.find(".grid-stub-row").trigger("click");
    await flushPromises();
    const drawer = wrapper.find(".drawer");
    expect(drawer.exists()).toBe(true);
    expect(drawer.find("header .mono").text()).toContain("TOPIC · order-events");
    expect(drawer.text()).toContain("User:billing");
    // 头部 ✕ 关闭
    await drawer.find("header .icon-button").trigger("click");
    expect(wrapper.find(".drawer").exists()).toBe(false);
  });
});
