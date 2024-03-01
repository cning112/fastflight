// @ts-check

import eslint from "@eslint/js";
import jestPlugin from "eslint-plugin-jest";
import react from "eslint-plugin-react";
import reactHooks from "eslint-plugin-react-hooks";
import tseslint from "typescript-eslint";

export default tseslint.config(
	{
		ignores: ["**/build/**", "**/dist/**"],
	},
	eslint.configs.recommended,
	...tseslint.configs.recommendedTypeChecked,
	...tseslint.configs.stylisticTypeChecked,
	{
		plugins: {
			"@typescript-eslint": tseslint.plugin,
			jest: jestPlugin,
			react: react,
			"react-hooks": reactHooks,
		},
		languageOptions: {
			parser: tseslint.parser,
			parserOptions: {
				project: true,
			},
		},
		rules: {
			"@typescript-eslint/no-unsafe-argument": "error",
			"@typescript-eslint/no-unsafe-assignment": "error",
			"@typescript-eslint/no-unsafe-call": "error",
			"@typescript-eslint/no-unsafe-member-access": "error",
			"@typescript-eslint/no-unsafe-return": "error",
			"react-hooks/rules-of-hooks": "error",
			"react-hooks/exhaustive-deps": "warn",
		},
	},
	{
		files: ["**/*.{js,jsx}"],
		extends: [tseslint.configs.disableTypeChecked],
	},
	{
		files: ["**/*.{ts,tsx}"],
		extends: ["react-app", "react-app/jest"],
	},
	{
		files: ["**/*.{ts,tsx,mts,cts}"],
		rules: {
			"no-undef": "off",
		},
	},
	{
		// enable jest rules on test files
		files: ["test/**"],
		...jestPlugin.configs["flat/recommended"],
	},
	{
		// this is an automatic config for storybook files
		extends: ["plugin:storybook/recommended"],
	},
);
