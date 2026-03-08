import { i as if_block, s as slot, a as set_attribute, b as set_class, f as set_style, r as rest_props, g as spread_props } from './i18n-CGlVUOvE.js';
import { M as push, t as template_effect, a as append, O as pop, S as sibling, R as from_html, Q as child, Z as get, T as reset, a2 as user_derived } from './index-Bq2njFOY.js';
import { S as Static } from './index-1GCmrSlD.js';
import './StreamingBar.svelte_svelte_type_style_lang-DWGn5tT_.js';
import { G as Gradio } from './utils.svelte-Bi9ASwv9.js';
import './ScrollFade.svelte_svelte_type_style_lang-D0JPkGar.js';
import './snippet-BG7qkY_1.js';
import './MarkdownCode.svelte_svelte_type_style_lang-B29zDiob.js';
import './prism-python-NrKIQnfs.js';
import './Clear-C8Tfa7QP.js';
import './html-Bwif0JPw.js';

var root = from_html(`<div><!> <!></div>`);

function Index($$anchor, $$props) {
	push($$props, true);

	// export let equal_height = true;
	// export let elem_id: string;
	// export let elem_classes: string[] = [];
	// export let visible: boolean | "hidden" = true;
	// export let variant: "default" | "panel" | "compact" = "default";
	// export let loading_status: LoadingStatus | undefined = undefined;
	// export let gradio: Gradio | undefined = undefined;
	// export let show_progress = false;
	// export let height: number | string | undefined;
	// export let min_height: number | string | undefined;
	// export let max_height: number | string | undefined;
	// export let scale: number | null = null;
	const get_dimension = (dimension_value) => {
		if (dimension_value === undefined) {
			return undefined;
		}

		if (typeof dimension_value === "number") {
			return dimension_value + "px";
		} else if (typeof dimension_value === "string") {
			return dimension_value;
		}
	};

	let props = rest_props($$props, ['$$slots', '$$events', '$$legacy']);
	let gradio = new Gradio(props);
	var div = root();
	let classes;
	let styles;
	var node = child(div);

	{
		var consequent = ($$anchor) => {
			{
				let $0 = user_derived(() => gradio.shared.loading_status
					? gradio.shared.loading_status.status == "pending" ? "generating" : gradio.shared.loading_status.status
					: null);

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
						get status() {
							return get($0);
						}
					}
				));
			}
		};

		if_block(node, ($$render) => {
			if (gradio.shared.loading_status && gradio.shared.loading_status.show_progress && gradio) $$render(consequent);
		});
	}

	var node_1 = sibling(node, 2);

	slot(node_1, $$props, 'default', {}, null);
	reset(div);

	template_effect(
		($0, $1) => {
			set_attribute(div, 'id', gradio.shared.elem_id);

			classes = set_class(div, 1, `row ${$0 ?? ''}`, 'svelte-7xavid', classes, {
				compact: gradio.props.variant === "compact",
				panel: gradio.props.variant === "panel",
				'unequal-height': gradio.props.equal_height === false,
				stretch: gradio.props.equal_height,
				hide: !gradio.shared.visible,
				'grow-children': gradio.shared.scale && gradio.shared.scale >= 1
			});

			styles = set_style(div, '', styles, $1);
		},
		[
			() => gradio.shared.elem_classes?.join(' '),
			() => ({
				height: get_dimension(gradio.props.height),
				'max-height': get_dimension(gradio.props.max_height),
				'min-height': get_dimension(gradio.props.min_height),
				'flex-grow': gradio.shared.scale
			})
		]
	);

	append($$anchor, div);
	pop();
}

export { Index as default };
//# sourceMappingURL=Index-Cyy7LoUW.js.map
