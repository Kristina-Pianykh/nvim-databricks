local BufferUtils = require("databricks.buffer")
local Config = require("databricks.config")
local Api = require("databricks.api")
local augroup = vim.api.nvim_create_augroup("ScratchBuffer", { clear = true })

local Databricks = {}
Databricks.__index = Databricks

function Databricks:new()
  local config = Config.get_default_config()

  local databricks = setmetatable({
    config = config,
    context_id = nil,
    creds = {},
    -- api = Api,
    buf = nil,
  }, self)

  -- Bind methods from Api to the Databricks instance
  for k, v in pairs(Api) do
    if type(v) == "function" then
      -- Bind the function to the instance
      databricks[k] = function(...)
        return v(databricks, ...)
      end
    end
  end

  return databricks
end

local databricks_instance = Databricks:new()

function Databricks.setup(self, partial_config)
  --handle function call with dot syntax as opposed to method with colon syntax
  -- databricks.setup(databricks_instance, partial_config) vs databricks_instance:setup(partial_config)
  if self ~= databricks_instance then
    partial_config = self
    self = databricks_instance
  end

  self.config = Config.merge_config(partial_config, self.config)

  print(vim.inspect(self.config))

  if not assert(self.config.settings.profile) then
    error("Databricks profile not set. Please set a profile in the config.")
  end
  if not assert(self.config.settings.cluster_id) then
    error("Databricks Cluster ID is not set. Please set it in the config.")
  end

  self.creds = Config.parse_databricks_config(
    self.config.settings.profile,
    self.config.settings.path
  )

  vim.api.nvim_create_autocmd("VimEnter", {
    group = augroup,
    desc = "Set a background buffer on load",
    once = true,
    callback = BufferUtils.create_buffer,
  })
  self.buf = vim.g.databricks_buf

  return self
end

return databricks_instance
