import { p as prop, i as if_block, b as set_class, a as set_attribute, f as set_style } from './i18n-CGlVUOvE.js';
import { M as push, ad as user_effect, t as template_effect, a as append, O as pop, S as sibling, R as from_html, Q as child, Z as get, a2 as user_derived, T as reset, ab as state, $ as set } from './index-Bq2njFOY.js';
import { a as action } from './actions-CgRQ2lHA.js';
import { c as css_units, b as copy } from './utils.svelte-Bi9ASwv9.js';
import { C as Check } from './Check-BKMHx_DF.js';
import { C as Copy } from './Copy-BQlJe6-D.js';
import { I as IconButton } from './ScrollFade.svelte_svelte_type_style_lang-D0JPkGar.js';
import { M as MarkdownCode } from './MarkdownCode-BYKuypS7.js';
import { I as IconButtonWrapper } from './IconButtonWrapper-DAsvkfJg.js';

var root = from_html(`<div data-testid="markdown"><!> <!></div>`);

function Markdown($$anchor, $$props) {
	push($$props, true);

	let elem_classes = prop($$props, 'elem_classes', 19, () => []),
		visible = prop($$props, 'visible', 3, true),
		min_height = prop($$props, 'min_height', 3, undefined),
		rtl = prop($$props, 'rtl', 3, false),
		sanitize_html = prop($$props, 'sanitize_html', 3, true),
		line_breaks = prop($$props, 'line_breaks', 3, false),
		latex_delimiters = prop($$props, 'latex_delimiters', 19, () => []),
		header_links = prop($$props, 'header_links', 3, false),
		height = prop($$props, 'height', 3, undefined),
		show_copy_button = prop($$props, 'show_copy_button', 3, false),
		loading_status = prop($$props, 'loading_status', 3, undefined),
		onchange = prop($$props, 'onchange', 3, () => {}),
		oncopy = prop($$props, 'oncopy', 3, (val) => {});

	let copied = state(false);
	let timer;

	user_effect(() => {
		if ($$props.value) {
			onchange()();
		}
	});

	async function handle_copy() {
		if ("clipboard" in navigator) {
			await navigator.clipboard.writeText($$props.value);
			oncopy()({ value: $$props.value });
			copy_feedback();
		}
	}

	function copy_feedback() {
		set(copied, true);

		if (timer) clearTimeout(timer);

		timer = setTimeout(
			() => {
				set(copied, false);
			},
			1000
		);
	}

	var div = root();
	let classes;
	let styles;
	var node = child(div);

	{
		var consequent = ($$anchor) => {
			IconButtonWrapper($$anchor, {
				children: ($$anchor, $$slotProps) => {
					{
						let $0 = user_derived(() => get(copied) ? Check : Copy);
						let $1 = user_derived(() => get(copied) ? "Copied conversation" : "Copy conversation");

						IconButton($$anchor, {
							get Icon() {
								return get($0);
							},
							onclick: handle_copy,
							get label() {
								return get($1);
							}
						});
					}
				},
				$$slots: { default: true }
			});
		};

		if_block(node, ($$render) => {
			if (show_copy_button()) $$render(consequent);
		});
	}

	var node_1 = sibling(node, 2);

	MarkdownCode(node_1, {
		get message() {
			return $$props.value;
		},

		get latex_delimiters() {
			return latex_delimiters();
		},

		get sanitize_html() {
			return sanitize_html();
		},

		get line_breaks() {
			return line_breaks();
		},
		chatbot: false,
		get header_links() {
			return header_links();
		},

		get theme_mode() {
			return $$props.theme_mode;
		}
	});

	reset(div);
	action(div, ($$node) => copy?.($$node));

	template_effect(
		($0, $1, $2) => {
			classes = set_class(div, 1, `prose ${$0 ?? ''}`, 'svelte-1xjkzpp', classes, { hide: !visible() });
			set_attribute(div, 'dir', rtl() ? "rtl" : "ltr");
			styles = set_style(div, $1, styles, $2);
			div.dir = div.dir;
		},
		[
			() => elem_classes()?.join(' ') || '',
			() => height()
				? `max-height: ${css_units(height())}; overflow-y: auto;`
				: "",

			() => ({
				'min-height': min_height() && loading_status()?.status !== "pending" ? css_units(min_height()) : undefined
			})
		]
	);

	append($$anchor, div);
	pop();
}

export { Markdown as M };
//# sourceMappingURL=Index.svelte_svelte_type_style_lang-BIr0F_Dm.js.map
