local StringUtil = require("databricks.strings")

local M = {}

M.parse_databricks_config = function(profile, config_path)
  local default_config_path = vim.fn.getenv("HOME") .. "/.databrickscfg"

  if config_path == nil then
    config_path = vim.fn.expand(default_config_path)
  else
    config_path = vim.fn.expand(config_path)
  end
  print(config_path)
  print(profile)

  --TODO: validate that config_path exists if not handle
  local creds = { host = nil, token = nil, profile = nil }

  for line in io.lines(config_path) do
    if creds.host ~= nil then
      if StringUtil.string_starts(line, "token") then
        local tmp = vim.split(line, " = ")
        creds.token = tmp[table.getn(tmp)]
        -- print(creds.token)
        break
      end
    end
    if creds.profile ~= nil then
      if StringUtil.string_starts(line, "host") then
        local tmp = vim.split(line, "https://")
        creds.host = tmp[table.getn(tmp)]
      end
    end
    if line == "[" .. profile .. "]" then
      creds.profile = profile
    end
  end

  -- print(creds.token)
  return creds
end

M.merge_config = function(partial_config, latest_config)
  partial_config = partial_config or {}
  local config = latest_config or M.get_default_config()
  for k, v in pairs(partial_config) do
    if k == "settings" then
      config.settings = vim.tbl_extend("force", config.settings, v)
    else
      config[k] = vim.tbl_extend("force", config[k] or {}, v) -- allow irrelevant keys
    end
  end
  return config
end

M.get_default_config = function()
  local default_config = {
    settings = {
      path = vim.fn.getenv("HOME") .. "/.databrickscfg",
      timeout = 20,
    },
  }
  -- print(default_config.settings.path)
  return default_config
end

return M
