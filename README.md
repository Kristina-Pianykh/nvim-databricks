# nvim-databricks

A Neovim Plugin for Python code execution on a remote Databricks cluster.

**⚠️DISCLAIMER: This plugic is in a very early stage of development and there's no plans of developing it further.⚠️**

## Context

This plugin was born out of pure suffering — the kind you get from editing code in the Databricks web UI. It started as a tiny proof-of-concept so I wouldn’t lose my mind, and… that’s about as far as it’s going. I switched jobs (praise be), so I no longer have to touch the Databricks shitshow of a web UI and therefore have zero incentive to polish this thing.

If you give the plugin a spin, you’ll quickly discover that running code on a cluster is currently a blocking operation. Translation: the whole thing freezes harder than your laptop at 2% battery when you throw a big file at it.

If that doesn’t scare you off and you actually want to make it better — be my guest. Fork it, twist it, teach it to fetch — whatever brings you joy.

## Dependencies

* [`plenary.nvim`](https://github.com/nvim-lua/plenary.nvim)
* [`databricks cli`](https://github.com/databricks/cli)

## Installation

Install this plugin using your favorite plugin manager.

Example using [`lazy.nvim`](https://github.com/folke/lazy.nvim):

```lua
{
    "Kristina-Pianykh/nvim-databricks",
    branch = "main",
    dependencies = {
      "nvim-lua/plenary.nvim",
    },
    config = function()
      local databricks = require("nvim-databricks")
      databricks:setup({
        settings = {
          profile    = <your_databricks_profile>,      -- required
          cluster_id = <your_databricks_cluster>,      -- required
          path       = <path_to_databricks_config>,    -- optional; defaults to $HOME/.databrickscfg
        },
      })

      -- your keybindings for executing code selected in the line visual code
      vim.keymap.set("v", "<leader>sp", function()
        databricks:launch()
      end, { noremap = true })

      -- your keybindings for clearing the execution context
      vim.keymap.set("n", "<leader>cl", function()
        databricks:clear_context()
      end, { noremap = true })
    end,
},
```

Make sure you have your Databricks profile set up and you're authenticated with your Databricks workspace. This requires [generating a personal access token](https://docs.databricks.com/en/dev-tools/cli/authentication.html#id1).
