import { f as set_style, s as slot, a as set_attribute, b as set_class, r as rest_props } from './i18n-CGlVUOvE.js';
import { M as push, t as template_effect, Z as get, a as append, O as pop, a2 as user_derived, Q as child, R as from_html, T as reset } from './index-Bq2njFOY.js';
import { G as Gradio } from './utils.svelte-Bi9ASwv9.js';

var root = from_html(`<div><div class="styler svelte-1p9262q"><!></div></div>`);

function Index($$anchor, $$props) {
	push($$props, true);

	const props = rest_props($$props, ['$$slots', '$$events', '$$legacy']);

	// Register the component with Gradio
	new Gradio(props);

	const elem_id = user_derived(() => $$props.elem_id || "");
	const elem_classes = user_derived(() => $$props.elem_classes || []);
	const visible = user_derived(() => $$props.visible === undefined ? true : $$props.visible);
	var div = root();
	let classes;
	var div_1 = child(div);

	set_style(div_1, '', {}, {
		'--block-radius': '0px',
		'--block-border-width': '0px',
		'--layout-gap': '1px',
		'--form-gap-width': '1px',
		'--button-border-width': '0px',
		'--button-large-radius': '0px',
		'--button-small-radius': '0px'
	});

	var node = child(div_1);

	slot(node, $$props, 'default', {}, null);
	reset(div_1);
	reset(div);

	template_effect(
		($0) => {
			set_attribute(div, 'id', get(elem_id));
			classes = set_class(div, 1, `gr-group ${$0 ?? ''}`, 'svelte-1p9262q', classes, { hide: !get(visible) });
		},
		[() => get(elem_classes).join(' ')]
	);

	append($$anchor, div);
	pop();
}

export { Index as default };
//# sourceMappingURL=Index-CG0iU8MX.js.map
