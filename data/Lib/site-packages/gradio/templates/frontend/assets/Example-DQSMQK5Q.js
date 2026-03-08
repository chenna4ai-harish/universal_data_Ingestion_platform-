import { p as prop, i as if_block, b as set_class, k as each, u as index } from './i18n-CGlVUOvE.js';
import { M as push, t as template_effect, a as append, O as pop, R as from_html, Q as child, N as first_child, T as reset, X as set_text, Z as get, S as sibling } from './index-Bq2njFOY.js';

var root_2 = from_html(`<li><code> </code></li>`);
var root_3 = from_html(`<li class="extra svelte-14aa7hi">...</li>`);
var root_1 = from_html(`<!> <!>`, 1);
var root = from_html(`<ul><!></ul>`);

function Example($$anchor, $$props) {
	push($$props, true);

	let selected = prop($$props, 'selected', 3, false);
	var ul = root();
	let classes;
	var node = child(ul);

	{
		var consequent_1 = ($$anchor) => {
			var fragment = root_1();
			var node_1 = first_child(fragment);

			each(node_1, 17, () => Array.isArray($$props.value) ? $$props.value.slice(0, 3) : [$$props.value], index, ($$anchor, path) => {
				var li = root_2();
				var code = child(li);
				var text = child(code);

				reset(code);
				reset(li);
				template_effect(() => set_text(text, `./${get(path) ?? ''}`));
				append($$anchor, li);
			});

			var node_2 = sibling(node_1, 2);

			{
				var consequent = ($$anchor) => {
					var li_1 = root_3();

					append($$anchor, li_1);
				};

				if_block(node_2, ($$render) => {
					if (Array.isArray($$props.value) && $$props.value.length > 3) $$render(consequent);
				});
			}

			append($$anchor, fragment);
		};

		if_block(node, ($$render) => {
			if ($$props.value) $$render(consequent_1);
		});
	}

	reset(ul);

	template_effect(() => classes = set_class(ul, 1, 'svelte-14aa7hi', null, classes, {
		table: $$props.type === "table",
		gallery: $$props.type === "gallery",
		selected: selected()
	}));

	append($$anchor, ul);
	pop();
}

export { Example as default };
//# sourceMappingURL=Example-DQSMQK5Q.js.map
