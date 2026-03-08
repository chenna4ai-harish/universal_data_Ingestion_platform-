import { p as prop, i as if_block, b as set_class } from './i18n-CGlVUOvE.js';
import { t as template_effect, a as append, R as from_html, Q as child, T as reset } from './index-Bq2njFOY.js';
import { J as JSON_1 } from './JSON-D22hEneH.js';
import './Check-BKMHx_DF.js';
import './Copy-BQlJe6-D.js';
import './ScrollFade.svelte_svelte_type_style_lang-D0JPkGar.js';
import './snippet-BG7qkY_1.js';
import './MarkdownCode.svelte_svelte_type_style_lang-B29zDiob.js';
import './prism-python-NrKIQnfs.js';
import './Empty-BrRNpJ3n.js';
import './IconButtonWrapper-DAsvkfJg.js';

var root = from_html(`<div><!></div>`);

function Example($$anchor, $$props) {
	let theme_mode = prop($$props, 'theme_mode', 3, "system"),
		selected = prop($$props, 'selected', 3, false);

	let show_indices = false;
	let label_height = 0;
	var div = root();
	let classes;
	var node = child(div);

	{
		var consequent = ($$anchor) => {
			JSON_1($$anchor, {
				get value() {
					return $$props.value;
				},
				open: true,
				get theme_mode() {
					return theme_mode();
				},
				show_indices,
				label_height,
				interactive: false,
				show_copy_button: false
			});
		};

		if_block(node, ($$render) => {
			if ($$props.value) $$render(consequent);
		});
	}

	reset(div);

	template_effect(() => classes = set_class(div, 1, 'container svelte-19cq9h3', null, classes, {
		table: $$props.type === "table",
		gallery: $$props.type === "gallery",
		selected: selected(),
		border: $$props.value
	}));

	append($$anchor, div);
}

export { Example as default };
//# sourceMappingURL=Example-CXb8xdQD.js.map
