import { p as prop, e as init, i as if_block } from './i18n-CGlVUOvE.js';
import { M as push, Q as child, S as sibling, N as first_child, T as reset, y as deep_read_state, w as untrack, t as template_effect, X as set_text, a as append, O as pop, P as flushSync, R as from_html } from './index-Bq2njFOY.js';
import { B as Button } from './Button-By21KHDn.js';
import './snippet-BG7qkY_1.js';
import './Image-XuMqTTwB.js';
import './misc-D8a4ZbMA.js';
/* empty css                                             */
import './ScrollFade.svelte_svelte_type_style_lang-D0JPkGar.js';
import './MarkdownCode.svelte_svelte_type_style_lang-B29zDiob.js';
import './prism-python-NrKIQnfs.js';
/* empty css                                                    */

var root_2 = from_html(`<span class="api-name svelte-1kdww8a"> </span>`);
var root_1 = from_html(`<div class="loading-dot self-baseline svelte-1kdww8a"></div> <p class="self-baseline svelte-1kdww8a">Recording API Calls:</p> <p class="self-baseline api-section svelte-1kdww8a"><span class="api-count svelte-1kdww8a"> </span> <!></p>`, 1);
var root = from_html(`<div id="api-recorder" class="svelte-1kdww8a"><!></div>`);

function ApiRecorder($$anchor, $$props) {
	push($$props, false);

	let api_calls = prop($$props, 'api_calls', 28, () => []);
	let dependencies = prop($$props, 'dependencies', 12);

	var $$exports = {
		get api_calls() {
			return api_calls();
		},

		set api_calls($$value) {
			api_calls($$value);
			flushSync();
		},

		get dependencies() {
			return dependencies();
		},

		set dependencies($$value) {
			dependencies($$value);
			flushSync();
		}
	};

	init();

	var div = root();
	var node = child(div);

	Button(node, {
		size: 'sm',
		variant: 'secondary',
		children: ($$anchor, $$slotProps) => {
			var fragment = root_1();
			var p = sibling(first_child(fragment), 4);
			var span = child(p);
			var text = child(span);

			reset(span);

			var node_1 = sibling(span, 2);

			{
				var consequent = ($$anchor) => {
					var span_1 = root_2();
					var text_1 = child(span_1);

					reset(span_1);

					template_effect(() => set_text(text_1, `/${(
						deep_read_state(dependencies()),
						deep_read_state(api_calls()),
						untrack(() => dependencies()[api_calls()[api_calls().length - 1].fn_index].api_name)
					) ?? ''}`));

					append($$anchor, span_1);
				};

				if_block(node_1, ($$render) => {
					if ((
						deep_read_state(api_calls()),
						untrack(() => api_calls().length > 0)
					)) $$render(consequent);
				});
			}

			reset(p);

			template_effect(() => set_text(text, `[${(
				deep_read_state(api_calls()),
				untrack(() => api_calls().length)
			) ?? ''}]`));

			append($$anchor, fragment);
		},
		$$slots: { default: true }
	});

	reset(div);
	append($$anchor, div);

	return pop($$exports);
}

export { ApiRecorder as default };
//# sourceMappingURL=ApiRecorder-PUzKIb4E.js.map
