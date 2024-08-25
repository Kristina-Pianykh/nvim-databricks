# nvim-databricks

A Neovim Plugin for Python code execution on a remote Databricks cluster.

!!! This a pre-beta version !!!

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
