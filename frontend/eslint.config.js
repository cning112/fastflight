// @ts-check

import eslint from "@eslint/js";
import jestPlugin from "eslint-plugin-jest";
import reactPlugin from "eslint-plugin-react";
import reactHooksPlugin from "eslint-plugin-react-hooks";
import tseslint from "typescript-eslint";
import storybookPlugin from "eslint-plugin-storybook";

export default tseslint.config(
	{
		ignores: ["**/build/**", "**/dist/**"],
	},
	eslint.configs.recommended,
	...tseslint.configs.recommendedTypeChecked,
	...tseslint.configs.stylisticTypeChecked,
	{
		plugins: {
			react: reactPlugin,
			"react-hooks": reactHooksPlugin,
			storybook: storybookPlugin,
		},
		languageOptions: {
			parserOptions: {
				project: true,
				tsconfigRootDir: import.meta.dirname,
			},
		},
	},
	{
		files: ["*.js"],
		...tseslint.configs.disableTypeChecked,
	},
	{
		// enable jest rules on test files
		files: ["test/**"],
		...jestPlugin.configs["flat/recommended"],
	},
);
