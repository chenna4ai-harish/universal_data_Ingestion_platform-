import { i as if_block, r as rest_props, g as spread_props } from './i18n-CGlVUOvE.js';
import { M as push, ab as state, ac as proxy, ad as user_effect, N as first_child, Z as get, a as append, S as sibling, a2 as user_derived, O as pop, $ as set, I as tick, R as from_html } from './index-Bq2njFOY.js';
import { G as Gradio, a as snapshot } from './utils.svelte-Bi9ASwv9.js';
import { T as Textbox } from './Textbox-BXTb2K73.js';
import { S as Static } from './index-1GCmrSlD.js';
import './StreamingBar.svelte_svelte_type_style_lang-DWGn5tT_.js';
import { B as Block } from './Block-DNK-JdLJ.js';
import './MarkdownCode.svelte_svelte_type_style_lang-B29zDiob.js';
import './ScrollFade.svelte_svelte_type_style_lang-D0JPkGar.js';
export { default as BaseExample } from './Example-DYo_cEEb.js';
import './actions-CgRQ2lHA.js';
import './input-CXpCw23l.js';
import './BlockTitle-Bcj082QO.js';
import './Info-CfqzbVCN.js';
import './MarkdownCode-BYKuypS7.js';
import './html-Bwif0JPw.js';
import './Check-BKMHx_DF.js';
import './Copy-BQlJe6-D.js';
import './Send-CFiHLThw.js';
import './Square-kMgNk9Wy.js';
import './IconButtonWrapper-DAsvkfJg.js';
import './snippet-BG7qkY_1.js';
import './Clear-C8Tfa7QP.js';
import './prism-python-NrKIQnfs.js';
import './size-CWi277d_.js';
/* empty css                                               */

var root_1 = from_html(`<!> <!>`, 1);

function Index($$anchor, $$props) {
	push($$props, true);

	let _props = rest_props($$props, ['$$slots', '$$events', '$$legacy']);
	const gradio = new Gradio(_props);
	let label = user_derived(() => gradio.shared.label || "Textbox");

	// Need to set the value to "" otherwise a change event gets
	// dispatched when the child sets it to ""
	gradio.props.value = gradio.props.value ?? "";

	let old_value = state(proxy(gradio.props.value));

	async function dispatch_change() {
		if (get(old_value) !== gradio.props.value) {
			set(old_value, gradio.props.value, true);
			await tick();
			gradio.dispatch("change", snapshot(gradio.props.value));
		}
	}

	async function handle_input(value) {
		if (!gradio.shared || !gradio.props) return;

		gradio.props.validation_error = null;
		gradio.props.value = value;
		await tick();
		gradio.dispatch("input");
	}

	user_effect(() => {
		dispatch_change();
	});

	function handle_change(value) {
		if (!gradio.shared || !gradio.props) return;

		gradio.props.validation_error = null;
		gradio.props.value = value;
	}

	Block($$anchor, {
		get visible() {
			return gradio.shared.visible;
		},

		get elem_id() {
			return gradio.shared.elem_id;
		},

		get elem_classes() {
			return gradio.shared.elem_classes;
		},

		get scale() {
			return gradio.shared.scale;
		},

		get min_width() {
			return gradio.shared.min_width;
		},
		allow_overflow: false,
		get padding() {
			return gradio.shared.container;
		},

		get rtl() {
			return gradio.props.rtl;
		},

		children: ($$anchor, $$slotProps) => {
			var fragment_1 = root_1();
			var node = first_child(fragment_1);

			{
				var consequent = ($$anchor) => {
					Static($$anchor, spread_props(
						{
							get autoscroll() {
								return gradio.shared.autoscroll;
							},

							get i18n() {
								return gradio.i18n;
							}
						},
						() => gradio.shared.loading_status,
						{
							show_validation_error: false,
							on_clear_status: () => gradio.dispatch("clear_status", gradio.shared.loading_status)
						}
					));
				};

				if_block(node, ($$render) => {
					if (gradio.shared.loading_status) $$render(consequent);
				});
			}

			var node_1 = sibling(node, 2);

			{
				let $0 = user_derived(() => gradio.shared?.loading_status?.validation_error || gradio.shared?.validation_error);
				let $1 = user_derived(() => !gradio.shared.interactive);

				Textbox(node_1, {
					get label() {
						return get(label);
					},

					get info() {
						return gradio.props.info;
					},

					get show_label() {
						return gradio.shared.show_label;
					},

					get lines() {
						return gradio.props.lines;
					},

					get type() {
						return gradio.props.type;
					},

					get rtl() {
						return gradio.props.rtl;
					},

					get text_align() {
						return gradio.props.text_align;
					},

					get max_lines() {
						return gradio.props.max_lines;
					},

					get placeholder() {
						return gradio.props.placeholder;
					},

					get submit_btn() {
						return gradio.props.submit_btn;
					},

					get stop_btn() {
						return gradio.props.stop_btn;
					},

					get buttons() {
						return gradio.props.buttons;
					},

					get autofocus() {
						return gradio.props.autofocus;
					},

					get container() {
						return gradio.shared.container;
					},

					get autoscroll() {
						return gradio.shared.autoscroll;
					},

					get max_length() {
						return gradio.props.max_length;
					},

					get html_attributes() {
						return gradio.props.html_attributes;
					},

					get validation_error() {
						return get($0);
					},
					onchange: handle_change,
					oninput: handle_input,
					onsubmit: () => {
						gradio.shared.validation_error = null;
						gradio.dispatch("submit");
					},
					onblur: () => gradio.dispatch("blur"),
					onselect: (data) => gradio.dispatch("select", data),
					onfocus: () => gradio.dispatch("focus"),
					onstop: () => gradio.dispatch("stop"),
					oncopy: (data) => gradio.dispatch("copy", data),
					oncustombuttonclick: (id) => {
						gradio.dispatch("custom_button_click", { id });
					},

					get disabled() {
						return get($1);
					},

					get value() {
						return gradio.props.value;
					},

					set value($$value) {
						gradio.props.value = $$value;
					}
				});
			}

			append($$anchor, fragment_1);
		},
		$$slots: { default: true }
	});

	pop();
}

export { Textbox as BaseTextbox, Index as default };
//# sourceMappingURL=Index-DIT5pDCQ.js.map
