import { g as spread_props, d as bind_this, r as rest_props, b as set_class } from './i18n-CGlVUOvE.js';
import { M as push, ad as user_effect, I as tick, N as first_child, Z as get, t as template_effect, a as append, S as sibling, Q as child, a2 as user_derived, O as pop, ab as state, $ as set, R as from_html, T as reset } from './index-Bq2njFOY.js';
import { G as Gradio, s as should_show_scroll_fade } from './utils.svelte-Bi9ASwv9.js';
import { M as Markdown } from './Index.svelte_svelte_type_style_lang-BIr0F_Dm.js';
import { S as Static } from './index-1GCmrSlD.js';
import './StreamingBar.svelte_svelte_type_style_lang-DWGn5tT_.js';
import { B as Block } from './Block-DNK-JdLJ.js';
import './MarkdownCode.svelte_svelte_type_style_lang-B29zDiob.js';
import './ScrollFade.svelte_svelte_type_style_lang-D0JPkGar.js';
import { S as ScrollFade } from './ScrollFade-CRDohWU-.js';
export { default as BaseExample } from './Example-DUe-V0Ld.js';
import './actions-CgRQ2lHA.js';
import './Check-BKMHx_DF.js';
import './Copy-BQlJe6-D.js';
import './MarkdownCode-BYKuypS7.js';
import './html-Bwif0JPw.js';
import './IconButtonWrapper-DAsvkfJg.js';
import './snippet-BG7qkY_1.js';
import './Clear-C8Tfa7QP.js';
import './prism-python-NrKIQnfs.js';
/* empty css                                               */

var root_1 = from_html(`<!> <div><!></div> <!>`, 1);

function Index($$anchor, $$props) {
	push($$props, true);

	let props = rest_props($$props, ['$$slots', '$$events', '$$legacy']);
	const gradio = new Gradio(props);
	let wrapper;
	let show_fade = state(false);

	function update_fade() {
		if (!gradio.props.height) return;

		set(show_fade, should_show_scroll_fade(wrapper?.closest(".block")), true);
	}

	user_effect(() => {
		const container = wrapper?.closest(".block");

		if (!container || !gradio.props.height) return;

		container.addEventListener("scroll", update_fade);
		tick().then(update_fade);

		return () => container.removeEventListener("scroll", update_fade);
	});

	user_effect(() => {
		if (gradio.props.value !== undefined) tick().then(update_fade);
	});

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

		get container() {
			return gradio.shared.container;
		},
		allow_overflow: true,
		overflow_behavior: 'auto',
		get height() {
			return gradio.props.height;
		},

		get min_height() {
			return gradio.props.min_height;
		},

		get max_height() {
			return gradio.props.max_height;
		},

		get rtl() {
			return gradio.props.rtl;
		},

		children: ($$anchor, $$slotProps) => {
			var fragment_1 = root_1();
			var node = first_child(fragment_1);

			Static(node, spread_props(
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
					variant: 'center',
					on_clear_status: () => gradio.dispatch("clear_status", gradio.shared.loading_status)
				}
			));

			var div = sibling(node, 2);
			let classes;
			var node_1 = child(div);

			{
				let $0 = user_derived(() => gradio.props.buttons?.includes("copy"));

				Markdown(node_1, {
					get value() {
						return gradio.props.value;
					},

					get elem_classes() {
						return gradio.shared.elem_classes;
					},

					get visible() {
						return gradio.shared.visible;
					},

					get rtl() {
						return gradio.props.rtl;
					},
					onchange: () => gradio.dispatch("change"),
					oncopy: (e) => gradio.dispatch("copy", e.detail),
					get latex_delimiters() {
						return gradio.props.latex_delimiters;
					},

					get sanitize_html() {
						return gradio.props.sanitize_html;
					},

					get line_breaks() {
						return gradio.props.line_breaks;
					},

					get header_links() {
						return gradio.props.header_links;
					},

					get show_copy_button() {
						return get($0);
					},

					get loading_status() {
						return gradio.shared.loading_status;
					},

					get theme_mode() {
						return gradio.shared.theme_mode;
					}
				});
			}

			reset(div);
			bind_this(div, ($$value) => wrapper = $$value, () => wrapper);

			var node_2 = sibling(div, 2);

			ScrollFade(node_2, {
				get visible() {
					return get(show_fade);
				}
			});

			template_effect(() => classes = set_class(div, 1, 'svelte-16ln60g', null, classes, {
				padding: gradio.props.padding,
				pending: gradio.shared.loading_status?.status === "pending" && gradio.shared.loading_status?.show_progress !== "hidden"
			}));

			append($$anchor, fragment_1);
		},
		$$slots: { default: true }
	});

	pop();
}

export { Markdown as BaseMarkdown, Index as default };
//# sourceMappingURL=Index-DzadCq8f.js.map
